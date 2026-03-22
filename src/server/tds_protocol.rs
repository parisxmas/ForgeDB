//! TDS (Tabular Data Stream) protocol server for SQL Server client compatibility.
//!
//! Implements enough of the TDS 7.4 protocol for Microsoft.Data.SqlClient to
//! connect, authenticate, and execute queries against ForgeDB.
//!
//! Packet layout: 8-byte header + payload
//!   [type:1][status:1][length:2 BE][spid:2 BE][packet_id:1][window:1]

use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read as IoRead, Write as IoWrite};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::database::Database;
use crate::tuple::types::Value;

// ---------------------------------------------------------------------------
// TDS packet types
// ---------------------------------------------------------------------------
const TDS_PRELOGIN: u8 = 18;
const TDS_LOGIN7: u8 = 16;
const TDS_SQL_BATCH: u8 = 1;
const TDS_RPC: u8 = 3;
const TDS_ATTENTION: u8 = 6;
const TDS_TRANS_MGR_REQ: u8 = 14;
const TDS_RESPONSE: u8 = 4;

// TDS token types
const TOKEN_ERROR: u8 = 0xAA;
const TOKEN_LOGINACK: u8 = 0xAD;
const TOKEN_ENVCHANGE: u8 = 0xE3;
const TOKEN_INFO: u8 = 0xAB;
const TOKEN_DONE: u8 = 0xFD;
const TOKEN_DONEPROC: u8 = 0xFE;
const TOKEN_DONEINPROC: u8 = 0xFF;
const TOKEN_COLMETADATA: u8 = 0x81;
const TOKEN_ROW: u8 = 0xD1;
const TOKEN_ORDER: u8 = 0xA9;

// DONE status flags
const DONE_FINAL: u16 = 0x0000;
const DONE_MORE: u16 = 0x0001;
const DONE_COUNT: u16 = 0x0010;

// TDS data type constants — nullable variants (BYTELEN prefix in ROW data)
const TDS_TYPE_INTN: u8 = 0x26;
const TDS_TYPE_NVARCHAR: u8 = 0xE7;
const TDS_TYPE_BITN: u8 = 0x68;
const TDS_TYPE_FLTN: u8 = 0x6D;
const TDS_TYPE_BIGVARCHAR: u8 = 0xA7;

// TDS data type constants — fixed variants (no length prefix in ROW data)
const TDS_TYPE_INT4: u8 = 0x38;   // SQLINT4 — fixed 4-byte signed integer
const TDS_TYPE_INT8: u8 = 0x7F;   // SQLINT8 — fixed 8-byte signed integer
const TDS_TYPE_FLT8: u8 = 0x3E;   // SQLFLT8 — fixed 8-byte float
const TDS_TYPE_BIT: u8 = 0x32;    // SQLBIT  — fixed 1-byte boolean

// Collation for NVARCHAR columns (code page 1252, sort ID 52 = Latin1_General_CI_AS)
const DEFAULT_COLLATION: [u8; 5] = [0x09, 0x04, 0xD0, 0x00, 0x34];

// ---------------------------------------------------------------------------
// TdsServer
// ---------------------------------------------------------------------------

pub struct TdsServer {
    db_path: String,
    bind_addr: String,
    /// Maximum number of concurrent connections (0 = unlimited)
    max_connections: u32,
}

impl TdsServer {
    pub fn new(db_path: &str, bind_addr: &str) -> Self {
        Self {
            db_path: db_path.to_string(),
            bind_addr: bind_addr.to_string(),
            max_connections: 0, // unlimited by default
        }
    }

    /// Set the maximum number of concurrent connections
    pub fn set_max_connections(&mut self, max: u32) {
        self.max_connections = max;
    }

    pub fn start(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.bind_addr)?;
        println!("TDS Server listening on {}", self.bind_addr);

        let db = Database::open(&self.db_path)
            .or_else(|_| Database::new(&self.db_path))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
        let db = Arc::new(db);

        let conn_counter = Arc::new(std::sync::atomic::AtomicU32::new(1));
        let active_connections = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let max_conns = self.max_connections;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    // Check connection limit
                    let current = active_connections.load(std::sync::atomic::Ordering::Relaxed);
                    if max_conns > 0 && current >= max_conns {
                        eprintln!("TDS: max connections ({}) reached, rejecting", max_conns);
                        drop(stream);
                        continue;
                    }

                    let _ = stream.set_nodelay(true);
                    let conn_id = conn_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let db = Arc::clone(&db);
                    let active = Arc::clone(&active_connections);
                    active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    thread::spawn(move || {
                        let mut handler = match TdsConnection::new(stream, db, conn_id) {
                            Ok(h) => h,
                            Err(e) => {
                                eprintln!("TDS conn {} init error: {}", conn_id, e);
                                active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                return;
                            }
                        };
                        if let Err(e) = handler.run() {
                            if e.kind() != io::ErrorKind::UnexpectedEof {
                                eprintln!("TDS conn {} error: {}", conn_id, e);
                            }
                        }
                        active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    });
                }
                Err(e) => eprintln!("TDS accept error: {}", e),
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RPC parameter value (parsed from TDS RPC payload)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum RpcParamValue {
    Null,
    Int(i32),
    BigInt(i64),
    Float(f64),
    Bit(bool),
    NVarchar(String),
}

impl RpcParamValue {
    /// Convert to a SQL literal string for parameter substitution.
    fn to_sql_literal(&self) -> String {
        match self {
            RpcParamValue::Null => "NULL".to_string(),
            RpcParamValue::Int(v) => v.to_string(),
            RpcParamValue::BigInt(v) => v.to_string(),
            RpcParamValue::Float(v) => {
                // Ensure we don't lose precision for whole numbers
                if *v == (*v as i64) as f64 && v.abs() < i64::MAX as f64 {
                    format!("{}.0", *v as i64)
                } else {
                    format!("{}", v)
                }
            }
            RpcParamValue::Bit(v) => if *v { "1".to_string() } else { "0".to_string() },
            RpcParamValue::NVarchar(s) => {
                // Escape single quotes inside the string
                let escaped = s.replace('\'', "''");
                format!("'{}'", escaped)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TdsConnection
// ---------------------------------------------------------------------------

struct TdsConnection {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    db: Arc<Database>,
    conn_id: u32,
    current_txn_id: Option<crate::common::TxnId>,
    packet_id: u8,
    /// Prepared statement handle counter
    next_prep_handle: i32,
    /// Maps prepared statement handle -> SQL text
    prepared_stmts: HashMap<i32, String>,
}

impl TdsConnection {
    fn new(stream: TcpStream, db: Arc<Database>, conn_id: u32) -> io::Result<Self> {
        let r = stream.try_clone()?;
        Ok(Self {
            reader: BufReader::with_capacity(8192, r),
            writer: BufWriter::with_capacity(65536, stream),
            db,
            conn_id,
            current_txn_id: None,
            packet_id: 1,
            next_prep_handle: 1,
            prepared_stmts: HashMap::new(),
        })
    }

    // -----------------------------------------------------------------------
    // Packet I/O
    // -----------------------------------------------------------------------

    fn read_packet(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let mut full_payload = Vec::new();
        loop {
            let mut hdr = [0u8; 8];
            self.reader.read_exact(&mut hdr)?;
            let pkt_type = hdr[0];
            let status = hdr[1];
            let length = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
            if length < 8 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "TDS packet too small"));
            }
            let payload_len = length - 8;
            let mut payload = vec![0u8; payload_len];
            self.reader.read_exact(&mut payload)?;
            full_payload.extend_from_slice(&payload);
            // status bit 0x01 = EOM (end of message)
            if status & 0x01 != 0 {
                return Ok((pkt_type, full_payload));
            }
            // Otherwise continue reading next packet of same message
            let _ = pkt_type; // multi-packet: all same type
        }
    }

    fn write_packet(&mut self, pkt_type: u8, payload: &[u8]) -> io::Result<()> {
        let length = (payload.len() + 8) as u16;
        if length > 4096 {
            return self.write_chunked(pkt_type, payload);
        }
        let mut hdr = [0u8; 8];
        hdr[0] = pkt_type;
        hdr[1] = 0x01; // EOM
        hdr[2] = (length >> 8) as u8;
        hdr[3] = (length & 0xFF) as u8;
        // SPID
        hdr[4] = (self.conn_id >> 8) as u8;
        hdr[5] = (self.conn_id & 0xFF) as u8;
        hdr[6] = self.packet_id;
        self.packet_id = self.packet_id.wrapping_add(1);
        hdr[7] = 0; // window

        self.writer.write_all(&hdr)?;
        self.writer.write_all(payload)?;
        self.writer.flush()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------

    fn run(&mut self) -> io::Result<()> {
        // Phase 1: PRELOGIN
        self.handle_prelogin()?;
        // Phase 2: LOGIN7
        self.handle_login()?;
        // Phase 3: SQL batches
        loop {
            let (pkt_type, payload) = match self.read_packet() {
                Ok(p) => p,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            };
            match pkt_type {
                TDS_SQL_BATCH => self.handle_sql_batch(&payload)?,
                TDS_RPC => {
                    self.handle_rpc(&payload)?;
                }
                TDS_TRANS_MGR_REQ => {
                    // Transaction manager request — send DONE.
                    let done = Self::build_done(DONE_FINAL, 0);
                    self.write_packet(TDS_RESPONSE, &done)?;
                }
                TDS_ATTENTION => {
                    // Client cancel — send DONE
                    let done = Self::build_done(DONE_FINAL, 0);
                    self.write_packet(TDS_RESPONSE, &done)?;
                }
                _ => {
                    // Unknown packet type — send DONE to avoid hanging the client
                    let done = Self::build_done(DONE_FINAL, 0);
                    self.write_packet(TDS_RESPONSE, &done)?;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // PRELOGIN
    // -----------------------------------------------------------------------

    fn handle_prelogin(&mut self) -> io::Result<()> {
        let (_pkt_type, _payload) = self.read_packet()?;

        // Build PRELOGIN response with:
        //   VERSION(0), ENCRYPTION(1), INSTOPT(2), THREADID(3), MARS(4), TERMINATOR
        let mut resp = Vec::new();

        // 5 options × 5 bytes each + 1 terminator = 26 bytes header
        let header_size: u16 = 5 * 5 + 1;
        let mut data_offset = header_size;

        // VERSION: 6 bytes data
        resp.push(0x00);
        resp.extend_from_slice(&data_offset.to_be_bytes());
        resp.extend_from_slice(&6u16.to_be_bytes());
        let ver_off = data_offset; data_offset += 6;

        // ENCRYPTION: 1 byte data
        resp.push(0x01);
        resp.extend_from_slice(&data_offset.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());
        let enc_off = data_offset; data_offset += 1;

        // INSTOPT: 1 byte data
        resp.push(0x02);
        resp.extend_from_slice(&data_offset.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());
        data_offset += 1;

        // THREADID: 4 bytes data
        resp.push(0x03);
        resp.extend_from_slice(&data_offset.to_be_bytes());
        resp.extend_from_slice(&4u16.to_be_bytes());
        data_offset += 4;

        // MARS: 1 byte data
        resp.push(0x04);
        resp.extend_from_slice(&data_offset.to_be_bytes());
        resp.extend_from_slice(&1u16.to_be_bytes());

        // TERMINATOR
        resp.push(0xFF);

        // --- Data area ---
        // VERSION: major=15, minor=0, build=2000, subbuild=0
        resp.extend_from_slice(&[15, 0, 0x07, 0xD0, 0x00, 0x00]);
        // ENCRYPTION: ENCRYPT_NOT_SUP (0x02) — skip SSL entirely
        resp.push(0x02);
        // INSTOPT: empty instance name
        resp.push(0x00);
        // THREADID: connection ID
        resp.extend_from_slice(&self.conn_id.to_be_bytes());
        // MARS: off (0x00)
        resp.push(0x00);

        self.write_packet(TDS_RESPONSE, &resp)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // LOGIN7
    // -----------------------------------------------------------------------

    fn handle_login(&mut self) -> io::Result<()> {
        let (pkt_type, payload) = self.read_packet()?;

        // Parse TDS version from Login7 (bytes 4-7 of payload, LE DWORD)
        let client_tds_ver = if payload.len() >= 8 {
            [payload[4], payload[5], payload[6], payload[7]]
        } else {
            [0x04, 0x00, 0x00, 0x74] // default TDS 7.4
        };

        // Build login response: ENVCHANGE + LOGINACK + DONE
        let mut resp = Vec::new();

        // ENVCHANGE: database (type 1)
        Self::append_envchange_database(&mut resp, "forgedb");

        // ENVCHANGE: packet size (type 4)
        Self::append_envchange_packet_size(&mut resp, "4096");

        // ENVCHANGE: collation (type 7) — required for SqlClient MetaType resolution
        // Without this, parameterized queries crash with NullReferenceException
        // Collation: Latin1_General_CI_AS = 0x0904D00034
        Self::append_envchange_collation(&mut resp);

        // LOGINACK
        Self::append_loginack(&mut resp, &client_tds_ver);

        // Check if client requested feature extensions (OptionFlags3 bit 4)
        let has_feature_ext = if payload.len() > 36 {
            payload[36] & 0x04 != 0 // fExtension bit
        } else {
            false
        };
        if has_feature_ext {
            resp.push(0xAE); // TOKEN_FEATUREEXTACK
            resp.push(0xFF); // terminator (no features)
        }

        // DONE (login complete)
        resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));

        self.write_packet(TDS_RESPONSE, &resp)?;
        Ok(())
    }

    fn parse_login7_database(payload: &[u8]) -> Option<String> {
        // Login7 packet has fixed 94-byte header, then variable-length fields
        // Database name offset/length at bytes 60-63
        if payload.len() < 94 {
            return None;
        }
        let db_offset = u16::from_le_bytes([payload[60], payload[61]]) as usize;
        let db_len = u16::from_le_bytes([payload[62], payload[63]]) as usize;
        if db_len == 0 || db_offset + db_len * 2 > payload.len() {
            return None;
        }
        let utf16: Vec<u16> = (0..db_len)
            .map(|i| u16::from_le_bytes([payload[db_offset + i * 2], payload[db_offset + i * 2 + 1]]))
            .collect();
        String::from_utf16(&utf16).ok()
    }

    // -----------------------------------------------------------------------
    // RPC handling (sp_executesql, sp_prepare, sp_execute, sp_unprepare)
    // -----------------------------------------------------------------------

    fn handle_rpc(&mut self, payload: &[u8]) -> io::Result<()> {
        let data = Self::skip_all_headers(payload);

        if data.len() < 4 {
            // Too short — send DONE
            let mut resp = Vec::new();
            Self::append_envchange_packet_size(&mut resp, "4096");
            resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
            self.write_packet(TDS_RESPONSE, &resp)?;
            return Ok(());
        }

        // Parse procedure ID or name
        let (proc_id, rest) = Self::parse_rpc_proc_id(data);

        // Skip OptionFlags (2 bytes)
        let params_data = if rest.len() >= 2 { &rest[2..] } else { rest };

        match proc_id {
            10 => self.handle_sp_executesql(params_data),
            11 => self.handle_sp_prepare(params_data),
            12 => self.handle_sp_execute(params_data),
            13 => self.handle_sp_unprepare(params_data),
            _ => {
                // Unknown RPC (e.g. sp_reset_connection) — send ENVCHANGE + DONE
                let mut resp = Vec::new();
                Self::append_envchange_packet_size(&mut resp, "4096");
                resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
                self.write_packet(TDS_RESPONSE, &resp)?;
                Ok(())
            }
        }
    }

    /// Parse the RPC procedure identifier from the payload.
    /// Returns (proc_id, remaining_data).
    /// proc_id: 10=sp_executesql, 11=sp_prepare, 12=sp_execute, 13=sp_unprepare
    /// Returns 0 for unknown procedures.
    fn parse_rpc_proc_id(data: &[u8]) -> (u16, &[u8]) {
        if data.len() < 2 {
            return (0, data);
        }

        let name_len = u16::from_le_bytes([data[0], data[1]]);

        if name_len == 0xFFFF {
            // ProcID follows as u16 LE
            if data.len() >= 4 {
                let id = u16::from_le_bytes([data[2], data[3]]);
                (id, &data[4..])
            } else {
                (0, data)
            }
        } else {
            // Named procedure: name_len chars of UTF-16LE
            let name_bytes = (name_len as usize) * 2;
            if data.len() >= 2 + name_bytes {
                let name_data = &data[2..2 + name_bytes];
                let name = Self::decode_utf16le(name_data).to_lowercase();
                let rest = &data[2 + name_bytes..];

                let id = match name.as_str() {
                    "sp_executesql" => 10,
                    "sp_prepare" => 11,
                    "sp_execute" => 12,
                    "sp_unprepare" => 13,
                    _ => 0,
                };
                (id, rest)
            } else {
                (0, data)
            }
        }
    }

    /// Parse a single RPC parameter from the data stream.
    /// Returns (param_value, remaining_data).
    fn parse_rpc_param<'a>(data: &'a [u8]) -> Option<(RpcParamValue, &'a [u8])> {
        if data.is_empty() {
            return None;
        }

        let mut pos = 0;

        // ParamName: B_VARCHAR (1 byte length in chars, then UTF-16LE)
        if pos >= data.len() { return None; }
        let name_len = data[pos] as usize;
        pos += 1;
        // Skip param name (name_len * 2 bytes of UTF-16LE)
        pos += name_len * 2;
        if pos > data.len() { return None; }

        // StatusFlags: 1 byte
        if pos >= data.len() { return None; }
        let _status = data[pos];
        pos += 1;

        // TypeInfo
        if pos >= data.len() { return None; }
        let type_id = data[pos];
        pos += 1;

        match type_id {
            // INTN (0x26) — nullable integer
            TDS_TYPE_INTN => {
                if pos >= data.len() { return None; }
                let max_len = data[pos] as usize;
                pos += 1;
                // Value: 1-byte actual length + data
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    let val = match actual_len {
                        4 => {
                            let v = i32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                            RpcParamValue::Int(v)
                        }
                        8 => {
                            let mut bytes = [0u8; 8];
                            bytes.copy_from_slice(&data[pos..pos+8]);
                            let v = i64::from_le_bytes(bytes);
                            RpcParamValue::BigInt(v)
                        }
                        2 => {
                            let v = i16::from_le_bytes([data[pos], data[pos+1]]);
                            RpcParamValue::Int(v as i32)
                        }
                        1 => {
                            RpcParamValue::Int(data[pos] as i32)
                        }
                        _ => RpcParamValue::Null,
                    };
                    pos += actual_len;
                    Some((val, &data[pos..]))
                }
            }
            // BITN (0x68) — nullable bit
            TDS_TYPE_BITN => {
                if pos >= data.len() { return None; }
                let _max_len = data[pos];
                pos += 1;
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos >= data.len() { return None; }
                    let v = data[pos] != 0;
                    pos += actual_len;
                    Some((RpcParamValue::Bit(v), &data[pos..]))
                }
            }
            // FLTN (0x6D) — nullable float
            TDS_TYPE_FLTN => {
                if pos >= data.len() { return None; }
                let _max_len = data[pos];
                pos += 1;
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    let val = if actual_len == 8 {
                        let mut bytes = [0u8; 8];
                        bytes.copy_from_slice(&data[pos..pos+8]);
                        RpcParamValue::Float(f64::from_le_bytes(bytes))
                    } else if actual_len == 4 {
                        let mut bytes = [0u8; 4];
                        bytes.copy_from_slice(&data[pos..pos+4]);
                        RpcParamValue::Float(f32::from_le_bytes(bytes) as f64)
                    } else {
                        RpcParamValue::Null
                    };
                    pos += actual_len;
                    Some((val, &data[pos..]))
                }
            }
            // NVARCHAR (0xE7) — Unicode string
            TDS_TYPE_NVARCHAR => {
                // MaxLength: 2 bytes LE
                if pos + 2 > data.len() { return None; }
                let max_len = u16::from_le_bytes([data[pos], data[pos+1]]);
                pos += 2;
                // Collation: 5 bytes
                if pos + 5 > data.len() { return None; }
                pos += 5;
                // Value: 2-byte length + UTF-16LE data
                if pos + 2 > data.len() { return None; }
                let actual_len = u16::from_le_bytes([data[pos], data[pos+1]]) as usize;
                pos += 2;
                if actual_len == 0xFFFF {
                    // NULL
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    let s = Self::decode_utf16le(&data[pos..pos+actual_len]);
                    pos += actual_len;
                    Some((RpcParamValue::NVarchar(s), &data[pos..]))
                }
            }
            // BIGVARCHAR (0xA7) — non-Unicode string
            TDS_TYPE_BIGVARCHAR => {
                // MaxLength: 2 bytes LE
                if pos + 2 > data.len() { return None; }
                let _max_len = u16::from_le_bytes([data[pos], data[pos+1]]);
                pos += 2;
                // Collation: 5 bytes
                if pos + 5 > data.len() { return None; }
                pos += 5;
                // Value: 2-byte length + data
                if pos + 2 > data.len() { return None; }
                let actual_len = u16::from_le_bytes([data[pos], data[pos+1]]) as usize;
                pos += 2;
                if actual_len == 0xFFFF {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    let s = String::from_utf8_lossy(&data[pos..pos+actual_len]).to_string();
                    pos += actual_len;
                    Some((RpcParamValue::NVarchar(s), &data[pos..]))
                }
            }
            // NTEXT/TEXT (0x63) — large text type used by some drivers
            0x63 => {
                // Skip: 4 byte maxlen + 5 byte collation
                if pos + 9 > data.len() { return None; }
                pos += 4 + 5;
                // Value: 4 byte text pointer length
                if pos + 4 > data.len() { return None; }
                let tp_len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                pos += 4;
                if tp_len == 0 || tp_len == 0xFFFFFFFF {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    // Skip text pointer + timestamp
                    pos += tp_len;
                    if pos + 8 > data.len() { return None; }
                    pos += 8; // timestamp
                    // actual data length
                    if pos + 4 > data.len() { return None; }
                    let data_len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                    pos += 4;
                    if pos + data_len > data.len() { return None; }
                    let s = Self::decode_utf16le(&data[pos..pos+data_len]);
                    pos += data_len;
                    Some((RpcParamValue::NVarchar(s), &data[pos..]))
                }
            }
            // DATETIMEN (0x6F) — nullable datetime
            0x6F => {
                if pos >= data.len() { return None; }
                let max_len = data[pos] as usize;
                pos += 1;
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    // Skip datetime bytes
                    if pos + actual_len > data.len() { return None; }
                    pos += actual_len;
                    Some((RpcParamValue::NVarchar("0".to_string()), &data[pos..]))
                }
            }
            // MONEYN (0x6E) — nullable money
            0x6E => {
                if pos >= data.len() { return None; }
                let _max_len = data[pos];
                pos += 1;
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    pos += actual_len;
                    Some((RpcParamValue::Float(0.0), &data[pos..]))
                }
            }
            // DECIMALN/NUMERICN (0x6A/0x6C) — nullable decimal
            0x6A | 0x6C => {
                if pos + 2 > data.len() { return None; }
                let _max_len = data[pos];
                let _precision = data[pos + 1];
                pos += 2;
                if pos >= data.len() { return None; }
                let _scale = data[pos];
                pos += 1;
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    // Parse decimal: first byte is sign (1=positive, 0=negative)
                    // remaining bytes are the integer value in LE
                    if actual_len >= 2 {
                        let sign = data[pos];
                        let int_bytes = &data[pos+1..pos+actual_len];
                        let mut val: i128 = 0;
                        for (i, &b) in int_bytes.iter().enumerate() {
                            val |= (b as i128) << (i * 8);
                        }
                        if sign == 0 { val = -val; }
                        // Convert to float, applying scale
                        let scale_factor = 10f64.powi(_scale as i32);
                        let fval = (val as f64) / scale_factor;
                        pos += actual_len;
                        Some((RpcParamValue::Float(fval), &data[pos..]))
                    } else {
                        pos += actual_len;
                        Some((RpcParamValue::Float(0.0), &data[pos..]))
                    }
                }
            }
            // UNIQUEIDENTIFIER (0x24) — nullable GUID
            0x24 => {
                if pos >= data.len() { return None; }
                let _max_len = data[pos];
                pos += 1;
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    // Format as GUID string
                    if actual_len == 16 {
                        let d = &data[pos..pos+16];
                        let guid = format!(
                            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                            d[3], d[2], d[1], d[0], d[5], d[4], d[7], d[6],
                            d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]
                        );
                        pos += actual_len;
                        Some((RpcParamValue::NVarchar(guid), &data[pos..]))
                    } else {
                        pos += actual_len;
                        Some((RpcParamValue::Null, &data[pos..]))
                    }
                }
            }
            // BIGVARBINARY (0xAD) — variable-length binary
            0xAD => {
                if pos + 2 > data.len() { return None; }
                let _max_len = u16::from_le_bytes([data[pos], data[pos+1]]);
                pos += 2;
                if pos + 2 > data.len() { return None; }
                let actual_len = u16::from_le_bytes([data[pos], data[pos+1]]) as usize;
                pos += 2;
                if actual_len == 0xFFFF {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    // Represent as hex string
                    let hex: String = data[pos..pos+actual_len].iter()
                        .map(|b| format!("{:02X}", b)).collect();
                    pos += actual_len;
                    Some((RpcParamValue::NVarchar(format!("X'{}'", hex)), &data[pos..]))
                }
            }
            // Fixed-length types (no max_len byte in TypeInfo)
            TDS_TYPE_INT4 => {
                // Fixed 4-byte integer
                if pos + 4 > data.len() { return None; }
                let v = i32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                pos += 4;
                Some((RpcParamValue::Int(v), &data[pos..]))
            }
            TDS_TYPE_INT8 => {
                if pos + 8 > data.len() { return None; }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&data[pos..pos+8]);
                let v = i64::from_le_bytes(bytes);
                pos += 8;
                Some((RpcParamValue::BigInt(v), &data[pos..]))
            }
            TDS_TYPE_FLT8 => {
                if pos + 8 > data.len() { return None; }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&data[pos..pos+8]);
                let v = f64::from_le_bytes(bytes);
                pos += 8;
                Some((RpcParamValue::Float(v), &data[pos..]))
            }
            TDS_TYPE_BIT => {
                if pos >= data.len() { return None; }
                let v = data[pos] != 0;
                pos += 1;
                Some((RpcParamValue::Bit(v), &data[pos..]))
            }
            // BIGCHAR (0xAF) — fixed-length non-Unicode string
            0xAF => {
                if pos + 2 > data.len() { return None; }
                let _max_len = u16::from_le_bytes([data[pos], data[pos+1]]);
                pos += 2;
                if pos + 5 > data.len() { return None; }
                pos += 5; // collation
                if pos + 2 > data.len() { return None; }
                let actual_len = u16::from_le_bytes([data[pos], data[pos+1]]) as usize;
                pos += 2;
                if actual_len == 0xFFFF {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    let s = String::from_utf8_lossy(&data[pos..pos+actual_len]).to_string();
                    pos += actual_len;
                    Some((RpcParamValue::NVarchar(s), &data[pos..]))
                }
            }
            // DATE (0x28) — 3-byte date
            0x28 => {
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    pos += actual_len;
                    Some((RpcParamValue::NVarchar("0".to_string()), &data[pos..]))
                }
            }
            // TIME (0x29), DATETIME2 (0x2A), DATETIMEOFFSET (0x2B)
            0x29 | 0x2A | 0x2B => {
                // These have a scale byte in TypeInfo
                if pos >= data.len() { return None; }
                let _scale = data[pos];
                pos += 1;
                if pos >= data.len() { return None; }
                let actual_len = data[pos] as usize;
                pos += 1;
                if actual_len == 0 {
                    Some((RpcParamValue::Null, &data[pos..]))
                } else {
                    if pos + actual_len > data.len() { return None; }
                    pos += actual_len;
                    Some((RpcParamValue::NVarchar("0".to_string()), &data[pos..]))
                }
            }
            _ => {
                // Unknown type — try to skip gracefully
                // For BYTELEN types (variable length with 1-byte max), try that pattern
                None
            }
        }
    }

    /// Parse all RPC parameters from the data stream.
    fn parse_all_rpc_params(data: &[u8]) -> Vec<RpcParamValue> {
        let mut params = Vec::new();
        let mut remaining = data;
        while !remaining.is_empty() {
            match Self::parse_rpc_param(remaining) {
                Some((val, rest)) => {
                    params.push(val);
                    remaining = rest;
                }
                None => break,
            }
        }
        params
    }

    /// sp_executesql: one-shot parameterized query.
    /// Params: [0] = SQL text, [1] = param definitions (optional), [2..] = param values
    fn handle_sp_executesql(&mut self, params_data: &[u8]) -> io::Result<()> {
        let params = Self::parse_all_rpc_params(params_data);

        let sql_template = match params.get(0) {
            Some(RpcParamValue::NVarchar(s)) => s.clone(),
            _ => {
                let mut resp = Vec::new();
                Self::append_error(&mut resp, 50000, "sp_executesql: missing SQL parameter");
                resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
                self.write_packet(TDS_RESPONSE, &resp)?;
                return Ok(());
            }
        };

        // Substitute parameters into the SQL template
        let final_sql = if params.len() > 2 {
            // params[1] = param definitions like "@p1 int, @p2 nvarchar(100)"
            // params[2..] = param values in order
            let param_defs = match params.get(1) {
                Some(RpcParamValue::NVarchar(s)) => s.clone(),
                _ => String::new(),
            };
            let param_names = Self::parse_param_definitions(&param_defs);
            Self::substitute_params(&sql_template, &param_names, &params[2..])
        } else {
            sql_template
        };

        // Execute using the SQL batch path (rewriting, interception, etc.)
        self.execute_rpc_sql(&final_sql)
    }

    /// sp_prepare: store SQL template, return handle.
    fn handle_sp_prepare(&mut self, params_data: &[u8]) -> io::Result<()> {
        let params = Self::parse_all_rpc_params(params_data);

        // sp_prepare params: [0] = handle (output), [1] = param definitions, [2] = SQL text
        // Or sometimes: [0] = param definitions, [1] = SQL text (handle returned as output)
        // SqlClient sends: @handle OUTPUT (int), @params nvarchar, @stmt nvarchar
        let sql_text = if params.len() >= 3 {
            match params.get(2) {
                Some(RpcParamValue::NVarchar(s)) => s.clone(),
                _ => match params.get(1) {
                    Some(RpcParamValue::NVarchar(s)) => s.clone(),
                    _ => String::new(),
                },
            }
        } else if params.len() >= 2 {
            match params.get(1) {
                Some(RpcParamValue::NVarchar(s)) => s.clone(),
                _ => match params.get(0) {
                    Some(RpcParamValue::NVarchar(s)) => s.clone(),
                    _ => String::new(),
                },
            }
        } else if params.len() == 1 {
            match params.get(0) {
                Some(RpcParamValue::NVarchar(s)) => s.clone(),
                _ => String::new(),
            }
        } else {
            String::new()
        };

        let handle = self.next_prep_handle;
        self.next_prep_handle += 1;
        self.prepared_stmts.insert(handle, sql_text);

        // Response: RETURNSTATUS(0) + result set with handle + DONEPROC
        let mut resp = Vec::new();

        // TOKEN_RETURNSTATUS (0x79) — return value = 0 (success)
        resp.push(0x79);
        resp.extend_from_slice(&0i32.to_le_bytes());

        // Return the handle as a single-column, single-row result set
        // This is what SqlClient reads via ExecuteScalar()
        let cols = vec!["handle".to_string()];
        let rows = vec![vec![Value::Integer(handle)]];
        Self::append_result_set(&mut resp, &cols, &rows);

        // DONEPROC instead of DONE for RPC results
        resp.push(TOKEN_DONEPROC);
        resp.extend_from_slice(&(DONE_FINAL | DONE_COUNT).to_le_bytes());
        resp.extend_from_slice(&0u16.to_le_bytes()); // CurCmd
        resp.extend_from_slice(&1i64.to_le_bytes()); // RowCount

        self.write_packet(TDS_RESPONSE, &resp)?;
        Ok(())
    }

    /// sp_execute: look up prepared handle, substitute params, execute.
    fn handle_sp_execute(&mut self, params_data: &[u8]) -> io::Result<()> {
        let params = Self::parse_all_rpc_params(params_data);

        // First param is the handle (INT)
        let handle = match params.get(0) {
            Some(RpcParamValue::Int(h)) => *h,
            Some(RpcParamValue::BigInt(h)) => *h as i32,
            _ => {
                let mut resp = Vec::new();
                Self::append_error(&mut resp, 50000, "sp_execute: invalid handle");
                resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
                self.write_packet(TDS_RESPONSE, &resp)?;
                return Ok(());
            }
        };

        let sql_template = match self.prepared_stmts.get(&handle) {
            Some(s) => s.clone(),
            None => {
                let mut resp = Vec::new();
                Self::append_error(&mut resp, 50000, &format!("sp_execute: handle {} not found", handle));
                resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
                self.write_packet(TDS_RESPONSE, &resp)?;
                return Ok(());
            }
        };

        // Remaining params are the parameter values.
        // We need to extract parameter names from the SQL template (@p1, @p2, etc.)
        let param_names = Self::extract_param_names_from_sql(&sql_template);
        let final_sql = if params.len() > 1 {
            Self::substitute_params(&sql_template, &param_names, &params[1..])
        } else {
            sql_template
        };

        self.execute_rpc_sql(&final_sql)
    }

    /// sp_unprepare: remove a prepared statement handle.
    fn handle_sp_unprepare(&mut self, params_data: &[u8]) -> io::Result<()> {
        let params = Self::parse_all_rpc_params(params_data);

        if let Some(RpcParamValue::Int(handle)) = params.get(0) {
            self.prepared_stmts.remove(handle);
        } else if let Some(RpcParamValue::BigInt(handle)) = params.get(0) {
            self.prepared_stmts.remove(&(*handle as i32));
        }

        // Send RETURNSTATUS(0) + DONEPROC
        let mut resp = Vec::new();
        resp.push(0x79); // TOKEN_RETURNSTATUS
        resp.extend_from_slice(&0i32.to_le_bytes());
        resp.push(TOKEN_DONEPROC);
        resp.extend_from_slice(&DONE_FINAL.to_le_bytes());
        resp.extend_from_slice(&0u16.to_le_bytes());
        resp.extend_from_slice(&0i64.to_le_bytes());

        self.write_packet(TDS_RESPONSE, &resp)?;
        Ok(())
    }

    /// Execute SQL from an RPC call (with T-SQL rewriting and interception).
    fn execute_rpc_sql(&mut self, sql: &str) -> io::Result<()> {
        let sql = sql.trim();
        if sql.is_empty() {
            let mut resp = Vec::new();
            resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
            self.write_packet(TDS_RESPONSE, &resp)?;
            return Ok(());
        }

        // Split on semicolons (same as SQL batch)
        let statements: Vec<&str> = if sql.contains(';') {
            sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect()
        } else {
            vec![sql]
        };

        let mut resp = Vec::new();

        for &trimmed in &statements {
            if trimmed.is_empty() {
                continue;
            }

            // Intercept T-SQL system queries
            if let Some(intercept_resp) = self.try_intercept_tds(trimmed) {
                resp.extend_from_slice(&intercept_resp);
                continue;
            }

            // Rewrite T-SQL
            let rewritten = Self::rewrite_tsql(trimmed);

            // Rewrite temp tables
            let final_sql = if rewritten.contains('#') {
                Self::rewrite_temp_tables(&rewritten, self.conn_id)
            } else {
                rewritten
            };

            let result = self.db.execute_sql_session(&final_sql, &mut self.current_txn_id);

            match result {
                Ok(exec_result) => {
                    if exec_result.columns.is_empty() {
                        resp.extend_from_slice(&Self::build_done(
                            DONE_FINAL | DONE_COUNT, exec_result.rows_affected as i64));
                    } else {
                        Self::append_result_set(&mut resp, &exec_result.columns, &exec_result.rows);
                        resp.extend_from_slice(&Self::build_done(
                            DONE_FINAL | DONE_COUNT, exec_result.rows.len() as i64));
                    }
                }
                Err(e) => {
                    Self::append_error(&mut resp, 50000, &format!("{}", e));
                    resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
                }
            }
        }

        if resp.is_empty() {
            resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
        }

        self.write_packet(TDS_RESPONSE, &resp)?;
        Ok(())
    }

    /// Parse parameter definitions string like "@p1 int, @p2 nvarchar(100)"
    /// Returns a list of parameter names in order.
    fn parse_param_definitions(defs: &str) -> Vec<String> {
        if defs.trim().is_empty() {
            return Vec::new();
        }
        let mut names = Vec::new();
        for part in defs.split(',') {
            let trimmed = part.trim();
            // First token is the parameter name (e.g. "@p1")
            if let Some(name) = trimmed.split_whitespace().next() {
                names.push(name.to_string());
            }
        }
        names
    }

    /// Extract @-parameter names from a SQL template by scanning for @name patterns.
    fn extract_param_names_from_sql(sql: &str) -> Vec<String> {
        let mut names = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '@' && i + 1 < chars.len() && (chars[i+1].is_alphanumeric() || chars[i+1] == '_') {
                // Not @@ (system variable)
                if i + 1 < chars.len() && chars[i+1] == '@' {
                    i += 2;
                    continue;
                }
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let name: String = chars[start..i].iter().collect();
                if !seen.contains(&name) {
                    seen.insert(name.clone());
                    names.push(name);
                }
            } else {
                i += 1;
            }
        }
        names
    }

    /// Substitute @param_name placeholders with actual values in the SQL string.
    fn substitute_params(sql: &str, param_names: &[String], param_values: &[RpcParamValue]) -> String {
        let mut result = sql.to_string();

        // Sort param names by length descending to avoid partial replacements
        // e.g. @p10 should be replaced before @p1
        let mut indexed: Vec<(usize, &String)> = param_names.iter().enumerate().collect();
        indexed.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

        for (idx, name) in indexed {
            if idx < param_values.len() {
                let replacement = param_values[idx].to_sql_literal();
                // Replace all occurrences of the parameter name
                // We need to be careful not to replace partial matches
                // e.g. @p1 should not be replaced inside @p10
                let mut new_result = String::with_capacity(result.len());
                let name_chars: Vec<char> = name.chars().collect();
                let result_chars: Vec<char> = result.chars().collect();
                let mut i = 0;
                while i < result_chars.len() {
                    if i + name_chars.len() <= result_chars.len() {
                        let slice: String = result_chars[i..i+name_chars.len()].iter().collect();
                        if slice.eq_ignore_ascii_case(name) {
                            // Check that the next character is NOT alphanumeric or underscore
                            let next_is_word = if i + name_chars.len() < result_chars.len() {
                                let c = result_chars[i + name_chars.len()];
                                c.is_alphanumeric() || c == '_'
                            } else {
                                false
                            };
                            if !next_is_word {
                                new_result.push_str(&replacement);
                                i += name_chars.len();
                                continue;
                            }
                        }
                    }
                    new_result.push(result_chars[i]);
                    i += 1;
                }
                result = new_result;
            }
        }
        result
    }

    // -----------------------------------------------------------------------
    // SQL Batch
    // -----------------------------------------------------------------------

    fn handle_sql_batch(&mut self, payload: &[u8]) -> io::Result<()> {
        let sql_bytes = Self::skip_all_headers(payload);
        let sql = Self::decode_utf16le(sql_bytes);
        let sql = sql.trim();

        if sql.is_empty() {
            let done = Self::build_done(DONE_FINAL, 0);
            self.write_packet(TDS_RESPONSE, &done)?;
            return Ok(());
        }

        // Fast path: single statement (no semicolons) — avoid split_statements allocation
        let statements: Vec<&str> = if sql.contains(';') {
            sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect()
        } else {
            vec![sql]
        };
        let mut resp = Vec::new();

        for (idx, &trimmed) in statements.iter().enumerate() {
            if trimmed.is_empty() {
                continue;
            }

            // Intercept T-SQL system queries from SqlClient
            if let Some(intercept_resp) = self.try_intercept_tds(trimmed) {
                resp.extend_from_slice(&intercept_resp);
                continue;
            }

            // Rewrite T-SQL to ForgeDB SQL (fast path skips when no T-SQL features)
            let rewritten = Self::rewrite_tsql(trimmed);

            // Rewrite temp table names (#table -> #tmp_connid__table)
            let final_sql = if rewritten.contains('#') {
                Self::rewrite_temp_tables(&rewritten, self.conn_id)
            } else {
                rewritten
            };

            let result = self.db.execute_sql_session(&final_sql, &mut self.current_txn_id);

            match result {
                Ok(exec_result) => {
                    if exec_result.columns.is_empty() {
                        // DML/DDL — no result set, just DONE with row count
                        resp.extend_from_slice(&Self::build_done(
                            DONE_FINAL | DONE_COUNT, exec_result.rows_affected as i64));
                    } else {
                        Self::append_result_set(&mut resp, &exec_result.columns, &exec_result.rows);
                        resp.extend_from_slice(&Self::build_done(
                            DONE_FINAL | DONE_COUNT, exec_result.rows.len() as i64));
                    }
                }
                Err(e) => {
                    Self::append_error(&mut resp, 50000, &format!("{}", e));
                    resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
                }
            }
        }

        if resp.is_empty() {
            resp.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
        }

        self.write_packet(TDS_RESPONSE, &resp)?;
        Ok(())
    }

    fn skip_all_headers(payload: &[u8]) -> &[u8] {
        // ALL_HEADERS: first 4 bytes = total length of headers section (LE u32)
        // If present, skip past it to get to the SQL text
        if payload.len() >= 4 {
            let total_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
            // Sanity: total_len should be >= 4 and <= payload.len()
            if total_len >= 4 && total_len <= payload.len() {
                return &payload[total_len..];
            }
        }
        payload
    }

    fn write_chunked(&mut self, pkt_type: u8, payload: &[u8]) -> io::Result<()> {
        let chunk_size = 4096 - 8; // max payload per packet
        let chunks: Vec<&[u8]> = payload.chunks(chunk_size).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let is_last = i == chunks.len() - 1;
            let length = (chunk.len() + 8) as u16;
            let mut hdr = [0u8; 8];
            hdr[0] = pkt_type;
            hdr[1] = if is_last { 0x01 } else { 0x00 }; // EOM on last
            hdr[2] = (length >> 8) as u8;
            hdr[3] = (length & 0xFF) as u8;
            hdr[4] = (self.conn_id >> 8) as u8;
            hdr[5] = (self.conn_id & 0xFF) as u8;
            hdr[6] = self.packet_id;
            self.packet_id = self.packet_id.wrapping_add(1);
            hdr[7] = 0;
            self.writer.write_all(&hdr)?;
            self.writer.write_all(chunk)?;
        }
        self.writer.flush()?;
        Ok(())
    }

    fn decode_utf16le(data: &[u8]) -> String {
        let words: Vec<u16> = data.chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        String::from_utf16_lossy(&words)
    }

    /// Rewrite temp table names (#name) to include connection ID
    fn rewrite_temp_tables(sql: &str, conn_id: u32) -> String {
        // Replace #tablename (but not ##global_temp) with #tmp_connid__tablename
        let mut result = String::with_capacity(sql.len());
        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '#' && i + 1 < chars.len() && chars[i + 1] != '#' && chars[i + 1].is_alphanumeric() {
                // Check that this isn't in the middle of a word
                let is_start = i == 0 || !chars[i - 1].is_alphanumeric();
                if is_start {
                    // Collect the table name
                    let mut name = String::new();
                    let mut j = i + 1;
                    while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                        name.push(chars[j]);
                        j += 1;
                    }
                    result.push_str(&format!("#tmp_{}_{}", conn_id, name));
                    i = j;
                    continue;
                }
            }
            result.push(chars[i]);
            i += 1;
        }
        result
    }

    fn split_statements(sql: &str) -> Vec<String> {
        // Split on semicolons, but not inside string literals
        let mut stmts = Vec::new();
        let mut current = String::new();
        let mut in_string = false;
        let mut chars = sql.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\'' {
                in_string = !in_string;
                current.push(ch);
            } else if ch == ';' && !in_string {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    stmts.push(trimmed);
                }
                current.clear();
            } else {
                current.push(ch);
            }
        }
        let trimmed = current.trim().to_string();
        if !trimmed.is_empty() {
            stmts.push(trimmed);
        }
        stmts
    }

    // -----------------------------------------------------------------------
    // T-SQL interception / rewriting
    // -----------------------------------------------------------------------

    fn try_intercept_tds(&self, sql: &str) -> Option<Vec<u8>> {
        let upper = sql.to_uppercase();

        // sp_executesql — strip the wrapper
        // Handled by rewrite_tsql instead

        // SET statements that SqlClient sends during init
        if upper.starts_with("SET ") {
            let mut buf = Vec::new();
            buf.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
            return Some(buf);
        }

        // @@VERSION
        if upper.contains("@@VERSION") {
            let mut buf = Vec::new();
            let cols = vec!["".to_string()];
            let rows = vec![vec![Value::Varchar(
                "Microsoft SQL Server 2019 (RTM) - 15.0.2000.5 - ForgeDB".to_string(),
            )]];
            Self::append_result_set(&mut buf, &cols, &rows);
            buf.extend_from_slice(&Self::build_done(DONE_FINAL | DONE_COUNT, 1));
            return Some(buf);
        }

        // @@SPID
        if upper.contains("@@SPID") {
            let mut buf = Vec::new();
            let cols = vec!["".to_string()];
            let rows = vec![vec![Value::Integer(self.conn_id as i32)]];
            Self::append_result_set(&mut buf, &cols, &rows);
            buf.extend_from_slice(&Self::build_done(DONE_FINAL | DONE_COUNT, 1));
            return Some(buf);
        }

        // sp_ system procs
        if upper.starts_with("EXEC SP_") || upper.starts_with("SP_") {
            let mut buf = Vec::new();
            buf.extend_from_slice(&Self::build_done(DONE_FINAL, 0));
            return Some(buf);
        }

        None
    }

    fn rewrite_tsql(sql: &str) -> String {
        // Fast path: simple INSERT/UPDATE/DELETE/SELECT with no T-SQL features
        // Skip all string replacements when no rewriting is needed
        let upper_prefix: String = sql.chars().take(12).collect::<String>().to_uppercase();
        if (upper_prefix.starts_with("INSERT") || upper_prefix.starts_with("UPDATE")
            || upper_prefix.starts_with("DELETE") || upper_prefix.starts_with("SELECT"))
            && !sql.contains('[') && !sql.contains("NVARCHAR") && !sql.contains("nvarchar")
            && !sql.contains("IDENTITY") && !sql.contains("GETDATE")
            && !sql.contains("ISNULL(") && !sql.contains("LEN(")
            && !sql.contains("NOLOCK") && !sql.contains(" TOP ")
        {
            return sql.to_string();
        }

        let mut s = sql.to_string();
        let upper = s.to_uppercase();

        // NVARCHAR -> VARCHAR
        s = s.replace("NVARCHAR", "VARCHAR").replace("nvarchar", "VARCHAR");

        // [dbo]. prefix removal
        s = s.replace("[dbo].", "").replace("[DBO].", "");

        // Square-bracket quoting -> no quoting
        s = s.replace('[', "").replace(']', "");

        // N'string' -> 'string'
        // Simple approach: replace N' with ' when preceded by non-alphanumeric
        let mut result = String::with_capacity(s.len());
        let chars: Vec<char> = s.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if (chars[i] == 'N' || chars[i] == 'n') && i + 1 < chars.len() && chars[i + 1] == '\'' {
                // Check if N is not part of a word
                let is_word_char = if i > 0 {
                    chars[i - 1].is_alphanumeric() || chars[i - 1] == '_'
                } else {
                    false
                };
                if !is_word_char {
                    // Skip the N, keep the '
                    i += 1;
                    continue;
                }
            }
            result.push(chars[i]);
            i += 1;
        }
        s = result;

        // IDENTITY(1,1) -> AUTO_INCREMENT
        let upper2 = s.to_uppercase();
        if let Some(pos) = upper2.find("IDENTITY") {
            let rest = &s[pos..];
            // Find the closing paren of IDENTITY(x,y)
            if let Some(paren_start) = rest.find('(') {
                if let Some(paren_end) = rest[paren_start..].find(')') {
                    let end = pos + paren_start + paren_end + 1;
                    s = format!("{}AUTO_INCREMENT{}", &s[..pos], &s[end..]);
                }
            } else {
                // IDENTITY without parens
                s = format!("{}AUTO_INCREMENT{}", &s[..pos], &s[pos + 8..]);
            }
        }

        // BIT -> BOOLEAN
        // Only replace standalone BIT type keywords, not inside words
        let mut out = String::new();
        let tokens: Vec<&str> = s.split_whitespace().collect();
        for (idx, tok) in tokens.iter().enumerate() {
            if idx > 0 { out.push(' '); }
            if tok.eq_ignore_ascii_case("BIT") {
                out.push_str("BOOLEAN");
            } else if tok.to_uppercase().starts_with("BIT,") {
                out.push_str("BOOLEAN,");
            } else {
                out.push_str(tok);
            }
        }
        s = out;

        // TOP N -> LIMIT N (move to end)
        let upper3 = s.to_uppercase();
        if upper3.starts_with("SELECT") && upper3.contains(" TOP ") {
            if let Some(top_pos) = upper3.find(" TOP ") {
                let after_top = &s[top_pos + 5..].trim_start();
                // Extract the number
                let num_end = after_top.find(|c: char| !c.is_ascii_digit()).unwrap_or(after_top.len());
                if num_end > 0 {
                    let num = &after_top[..num_end];
                    let rest = after_top[num_end..].trim_start();
                    s = format!("{} {} LIMIT {}", &s[..top_pos], rest, num);
                }
            }
        }

        // GETDATE() -> 0 (placeholder)
        s = s.replace("GETDATE()", "0").replace("getdate()", "0");

        // ISNULL(a, b) -> COALESCE(a, b)
        s = s.replace("ISNULL(", "COALESCE(").replace("isnull(", "COALESCE(");

        // LEN( -> LENGTH(
        s = s.replace("LEN(", "LENGTH(").replace("len(", "LENGTH(");

        // SCOPE_IDENTITY() -> 0
        s = s.replace("SCOPE_IDENTITY()", "0").replace("scope_identity()", "0");

        // Strip WITH (NOLOCK) hints
        s = s.replace("WITH (NOLOCK)", "").replace("WITH(NOLOCK)", "")
             .replace("with (nolock)", "").replace("(NOLOCK)", "").replace("(nolock)", "");

        // PRINT -> ignore
        if s.to_uppercase().starts_with("PRINT ") {
            return String::new();
        }

        // INCREMENT BY n -> INCREMENT n (for CREATE SEQUENCE)
        s = s.replace("INCREMENT BY ", "INCREMENT ").replace("increment by ", "INCREMENT ");

        // START WITH n -> START n (for CREATE SEQUENCE)
        s = s.replace("START WITH ", "START ").replace("start with ", "START ");

        // MINVALUE / MAXVALUE / NO CYCLE / CACHE -> strip (CREATE SEQUENCE options)
        let upper_final = s.to_uppercase();
        if upper_final.contains("CREATE SEQUENCE") {
            s = s.replace("NO CYCLE", "").replace("NO MINVALUE", "")
                 .replace("NO MAXVALUE", "").replace("NO CACHE", "");
            // Remove MINVALUE n, MAXVALUE n, CACHE n
            let re_strip = ["MINVALUE", "MAXVALUE", "CACHE"];
            for kw in &re_strip {
                if let Some(pos) = s.to_uppercase().find(kw) {
                    let after = &s[pos + kw.len()..].trim_start();
                    let num_end = after.find(|c: char| !c.is_ascii_digit() && c != '-').unwrap_or(after.len());
                    s = format!("{}{}", &s[..pos], &after[num_end..]);
                }
            }
        }

        // UPDATE t1 SET ... FROM t1 JOIN t2 ON ... WHERE ...
        // Rewrite to: UPDATE t1 SET ... WHERE EXISTS (SELECT 1 FROM t2 WHERE ...)
        let upper_tsql = s.to_uppercase();
        if upper_tsql.starts_with("UPDATE ") && upper_tsql.contains(" FROM ") && upper_tsql.contains(" JOIN ") {
            // Try to rewrite: UPDATE t SET t.col = t2.val FROM t JOIN t2 ON t.id = t2.id WHERE ...
            // Simplify: just strip the FROM clause and let the parser handle UPDATE SET WHERE
            if let Some(from_pos) = upper_tsql.find(" FROM ") {
                let set_pos = upper_tsql.find(" SET ").unwrap_or(0);
                if from_pos > set_pos {
                    // Find WHERE after FROM
                    let after_from = &s[from_pos + 6..];
                    let where_in_from = after_from.to_uppercase().find(" WHERE ");
                    if let Some(wp) = where_in_from {
                        let where_clause = &after_from[wp..];
                        let before_from = &s[..from_pos];
                        s = format!("{}{}", before_from, where_clause);
                    } else {
                        // No WHERE, just strip FROM clause
                        s = s[..from_pos].to_string();
                    }
                }
            }
        }

        // DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...
        // Rewrite to: DELETE FROM t1 WHERE ...
        let upper_tsql2 = s.to_uppercase();
        if upper_tsql2.starts_with("DELETE ") && !upper_tsql2.starts_with("DELETE FROM ") {
            if let Some(from_pos) = upper_tsql2.find(" FROM ") {
                // Extract target table (between DELETE and FROM)
                let target = s[7..from_pos].trim().to_string();
                let after_from = &s[from_pos + 6..];
                // Find WHERE in the rest
                let where_in_rest = after_from.to_uppercase().find(" WHERE ");
                if let Some(wp) = where_in_rest {
                    let where_clause = &after_from[wp..];
                    s = format!("DELETE FROM {}{}", target, where_clause);
                } else {
                    s = format!("DELETE FROM {}", target);
                }
            }
        }

        s.trim().to_string()
    }

    // -----------------------------------------------------------------------
    // Token builders
    // -----------------------------------------------------------------------

    fn build_done(status: u16, row_count: i64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(13);
        buf.push(TOKEN_DONE);
        buf.extend_from_slice(&status.to_le_bytes()); // Status
        buf.extend_from_slice(&0u16.to_le_bytes()); // CurCmd
        buf.extend_from_slice(&row_count.to_le_bytes()); // DoneRowCount (8 bytes, LE)
        buf
    }

    fn append_error(buf: &mut Vec<u8>, number: i32, message: &str) {
        let msg_utf16: Vec<u16> = message.encode_utf16().collect();
        let msg_bytes: Vec<u8> = msg_utf16.iter().flat_map(|w| w.to_le_bytes()).collect();

        let server = "ForgeDB";
        let server_utf16: Vec<u16> = server.encode_utf16().collect();
        let server_bytes: Vec<u8> = server_utf16.iter().flat_map(|w| w.to_le_bytes()).collect();

        // length = 4(number) + 1(state) + 1(class) + 2(msglen) + msg + 1(serverlen) + server + 1(proclen) + 4(line)
        let token_len = 4 + 1 + 1 + 2 + msg_bytes.len() + 1 + server_bytes.len() + 1 + 4;

        buf.push(TOKEN_ERROR);
        buf.extend_from_slice(&(token_len as u16).to_le_bytes());
        buf.extend_from_slice(&number.to_le_bytes()); // Number
        buf.push(1); // State
        buf.push(16); // Class (severity)
        buf.extend_from_slice(&(msg_utf16.len() as u16).to_le_bytes()); // MsgText length (chars)
        buf.extend_from_slice(&msg_bytes); // MsgText
        buf.push(server_utf16.len() as u8); // ServerName length
        buf.extend_from_slice(&server_bytes); // ServerName
        buf.push(0); // ProcName length
        buf.extend_from_slice(&0u32.to_le_bytes()); // LineNumber
    }

    fn append_loginack(buf: &mut Vec<u8>, _client_tds_ver: &[u8; 4]) {
        let prog_name = "ForgeDB";
        let prog_utf16: Vec<u16> = prog_name.encode_utf16().collect();
        let prog_bytes: Vec<u8> = prog_utf16.iter().flat_map(|w| w.to_le_bytes()).collect();

        // LOGINACK: interface(1) + tds_version(4) + progname_len(1) + progname + version(4)
        let token_len = 1 + 4 + 1 + prog_bytes.len() + 4;

        buf.push(TOKEN_LOGINACK);
        buf.extend_from_slice(&(token_len as u16).to_le_bytes());
        buf.push(1); // Interface: SQL_TSQL
        // TDS version in BE byte order: SqlClient reads 4 bytes MSB-first
        // TDS 7.4 (DENALI) = 0x74000004 → [0x74, 0x00, 0x00, 0x04]
        buf.extend_from_slice(&[0x74, 0x00, 0x00, 0x04]);
        buf.push(prog_utf16.len() as u8); // ProgName length (chars)
        buf.extend_from_slice(&prog_bytes);
        buf.extend_from_slice(&[15, 0, 0x07, 0xD0]); // Server version 15.0.2000
    }

    fn append_envchange_database(buf: &mut Vec<u8>, db_name: &str) {
        let name_utf16: Vec<u16> = db_name.encode_utf16().collect();
        let name_bytes: Vec<u8> = name_utf16.iter().flat_map(|w| w.to_le_bytes()).collect();

        // Type(1) + NewValueLen(1) + NewValue + OldValueLen(1) + OldValue
        let data_len = 1 + 1 + name_bytes.len() + 1 + name_bytes.len();

        buf.push(TOKEN_ENVCHANGE);
        buf.extend_from_slice(&(data_len as u16).to_le_bytes());
        buf.push(1); // Type: Database
        buf.push(name_utf16.len() as u8);
        buf.extend_from_slice(&name_bytes);
        buf.push(name_utf16.len() as u8);
        buf.extend_from_slice(&name_bytes);
    }

    fn append_envchange_packet_size(buf: &mut Vec<u8>, size: &str) {
        let size_utf16: Vec<u16> = size.encode_utf16().collect();
        let size_bytes: Vec<u8> = size_utf16.iter().flat_map(|w| w.to_le_bytes()).collect();

        let data_len = 1 + 1 + size_bytes.len() + 1 + size_bytes.len();

        buf.push(TOKEN_ENVCHANGE);
        buf.extend_from_slice(&(data_len as u16).to_le_bytes());
        buf.push(4); // Type: Packet Size
        buf.push(size_utf16.len() as u8);
        buf.extend_from_slice(&size_bytes);
        buf.push(size_utf16.len() as u8);
        buf.extend_from_slice(&size_bytes);
    }

    /// ENVCHANGE type 7: SQL Collation.
    /// SqlClient uses this to resolve MetaType for string parameters.
    /// Without it, parameterized queries crash with NullReferenceException.
    fn append_envchange_collation(buf: &mut Vec<u8>) {
        // ENVCHANGE type 7: Collation
        // Format: Type(1) + NewLen(1) + NewCollation(5) + OldLen(1) + OldCollation(5)
        // Collation bytes: Latin1_General_CI_AS = [0x09, 0x04, 0xD0, 0x00, 0x34]
        let collation = &DEFAULT_COLLATION;
        let data_len = 1 + 1 + 5 + 1 + 5; // type + newlen + new + oldlen + old
        buf.push(TOKEN_ENVCHANGE);
        buf.extend_from_slice(&(data_len as u16).to_le_bytes());
        buf.push(7); // Type: SQL Collation
        buf.push(5); // NewValue length
        buf.extend_from_slice(collation);
        buf.push(5); // OldValue length
        buf.extend_from_slice(collation);
    }

    // -----------------------------------------------------------------------
    // Result set encoding
    // -----------------------------------------------------------------------

    fn append_result_set(buf: &mut Vec<u8>, columns: &[String], rows: &[Vec<Value>]) {
        let num_cols = columns.len();

        // -----------------------------------------------------------------
        // Pass 1: Detect the best TDS type for each column by scanning all
        // rows.  The result is a (tds_type, max_length) pair per column.
        // -----------------------------------------------------------------
        let col_meta: Vec<(u8, u8)> = (0..num_cols)
            .map(|col_idx| Self::detect_column_type(rows, col_idx))
            .collect();

        // -----------------------------------------------------------------
        // COLMETADATA token
        // -----------------------------------------------------------------
        buf.push(TOKEN_COLMETADATA);
        buf.extend_from_slice(&(num_cols as u16).to_le_bytes());

        for (i, col_name) in columns.iter().enumerate() {
            let (tds_type, _max_len) = col_meta[i];

            // UserType (4 bytes LE)
            buf.extend_from_slice(&0u32.to_le_bytes());

            // Flags (2 bytes, read as 2 separate bytes by SqlClient)
            // Byte 0: bit 0 = fNullable, bit 1..2 = Updatability, bit 4 = fIdentity
            // Byte 1: bit 0 = fClrFixedLen, bit 2 = fColumnSet, bit 3 = fEncrypted
            let flag_byte0: u8 = match tds_type {
                // Nullable types must have bit 0 set so SqlClient stores NullableType
                TDS_TYPE_INTN | TDS_TYPE_FLTN | TDS_TYPE_BITN | TDS_TYPE_NVARCHAR => 0x01,
                // Fixed types are NOT nullable
                _ => 0x00,
            };
            buf.push(flag_byte0);
            buf.push(0x00); // flag byte 1

            // TYPE_INFO — type-specific encoding
            match tds_type {
                // --- Nullable types (with MaxLength byte) ---
                TDS_TYPE_INTN => {
                    buf.push(TDS_TYPE_INTN);
                    buf.push(_max_len); // 4 or 8
                }
                TDS_TYPE_FLTN => {
                    buf.push(TDS_TYPE_FLTN);
                    buf.push(8);
                }
                TDS_TYPE_BITN => {
                    buf.push(TDS_TYPE_BITN);
                    buf.push(1);
                }
                // --- Fixed types (no MaxLength byte) ---
                TDS_TYPE_INT4 => {
                    buf.push(TDS_TYPE_INT4);
                    // SQLINT4: fixed 4-byte — no length byte in metadata
                }
                TDS_TYPE_INT8 => {
                    buf.push(TDS_TYPE_INT8);
                    // SQLINT8: fixed 8-byte — no length byte in metadata
                }
                TDS_TYPE_FLT8 => {
                    buf.push(TDS_TYPE_FLT8);
                    // SQLFLT8: fixed 8-byte — no length byte in metadata
                }
                TDS_TYPE_BIT => {
                    buf.push(TDS_TYPE_BIT);
                    // SQLBIT: fixed 1-byte — no length byte in metadata
                }
                _ => {
                    // NVARCHAR (safe default)
                    buf.push(TDS_TYPE_NVARCHAR);
                    buf.extend_from_slice(&8000u16.to_le_bytes());
                    buf.extend_from_slice(&DEFAULT_COLLATION);
                }
            }

            // Column name (1-byte char count + UTF-16LE)
            let name_utf16: Vec<u16> = col_name.encode_utf16().collect();
            let name_bytes: Vec<u8> = name_utf16.iter().flat_map(|w| w.to_le_bytes()).collect();
            buf.push(name_utf16.len() as u8);
            buf.extend_from_slice(&name_bytes);
        }

        // -----------------------------------------------------------------
        // ROW tokens
        // -----------------------------------------------------------------
        for row in rows {
            buf.push(TOKEN_ROW);
            for (i, val) in row.iter().enumerate() {
                let (tds_type, max_len) = if i < col_meta.len() {
                    col_meta[i]
                } else {
                    (TDS_TYPE_NVARCHAR, 0)
                };
                Self::encode_value_binary(buf, val, tds_type, max_len);
            }
        }
    }

    /// Scan all rows for a given column index and determine the best TDS
    /// wire type.  Returns `(tds_type_id, max_length)`.
    ///
    /// We use FIXED type IDs (SQLINT4/INT8/BIT) for columns that have
    /// no NULLs, because SqlClient reads these without a length prefix.
    /// For columns that contain NULLs, we use the NULLABLE type IDs
    /// (SQLINTN/BITN) which use a 1-byte length prefix in ROW data.
    ///
    /// Float and other non-integer types always use NVARCHAR to avoid
    /// locale-dependent `Double.ToString()` issues in client-side parsing.
    fn detect_column_type(rows: &[Vec<Value>], col_idx: usize) -> (u8, u8) {
        // Tracks what value kinds we have seen.
        let mut has_integer = false;
        let mut has_bigint = false;
        let mut has_boolean = false;
        let mut has_null = false;
        let mut has_other = false; // Float / Varchar / DateTime / Decimal / etc.
        let mut all_null = true;

        for row in rows {
            let val = if col_idx < row.len() { &row[col_idx] } else { &Value::Null };
            match val {
                Value::Null => { has_null = true; }
                Value::Integer(_) => { all_null = false; has_integer = true; }
                Value::BigInt(_) => { all_null = false; has_bigint = true; }
                Value::Boolean(_) => { all_null = false; has_boolean = true; }
                // Float and all other types → NVARCHAR (avoids locale issues)
                _ => { all_null = false; has_other = true; }
            }
        }

        if all_null || has_other {
            return (TDS_TYPE_NVARCHAR, 0);
        }

        // If any NULL is present, use nullable types (1-byte length prefix).
        if has_null {
            if has_boolean && !has_integer && !has_bigint {
                return (TDS_TYPE_BITN, 1);
            }
            if (has_integer || has_bigint) && !has_boolean {
                if has_bigint {
                    return (TDS_TYPE_INTN, 8);
                } else {
                    return (TDS_TYPE_INTN, 4);
                }
            }
            return (TDS_TYPE_NVARCHAR, 0);
        }

        // No NULLs — use FIXED types (no length prefix in ROW data).

        // Pure boolean column
        if has_boolean && !has_integer && !has_bigint {
            return (TDS_TYPE_BIT, 1);
        }

        // Integer family
        if (has_integer || has_bigint) && !has_boolean {
            if has_bigint {
                return (TDS_TYPE_INT8, 8);
            } else {
                return (TDS_TYPE_INT4, 4);
            }
        }

        // Mixed types — fall back to NVARCHAR
        (TDS_TYPE_NVARCHAR, 0)
    }

    #[inline]
    fn encode_value_binary(buf: &mut Vec<u8>, val: &Value, tds_type: u8, max_len: u8) {
        match tds_type {
            // ----- Fixed types: raw bytes, no length prefix, no NULLs -----
            TDS_TYPE_INT4 => {
                match val {
                    Value::Integer(n) => buf.extend_from_slice(&n.to_le_bytes()),
                    Value::BigInt(n) => buf.extend_from_slice(&(*n as i32).to_le_bytes()),
                    _ => buf.extend_from_slice(&0i32.to_le_bytes()), // shouldn't happen
                }
            }
            TDS_TYPE_INT8 => {
                match val {
                    Value::BigInt(n) => buf.extend_from_slice(&n.to_le_bytes()),
                    Value::Integer(n) => buf.extend_from_slice(&(*n as i64).to_le_bytes()),
                    _ => buf.extend_from_slice(&0i64.to_le_bytes()),
                }
            }
            TDS_TYPE_FLT8 => {
                match val {
                    Value::Float(f) => buf.extend_from_slice(&f.to_le_bytes()),
                    _ => buf.extend_from_slice(&0f64.to_le_bytes()),
                }
            }
            TDS_TYPE_BIT => {
                match val {
                    Value::Boolean(b) => buf.push(if *b { 1 } else { 0 }),
                    _ => buf.push(0),
                }
            }
            // ----- Nullable types: 1-byte length prefix -----
            TDS_TYPE_INTN => {
                match val {
                    Value::Null => buf.push(0), // length=0 means NULL
                    Value::Integer(n) => {
                        if max_len == 8 {
                            buf.push(8);
                            buf.extend_from_slice(&(*n as i64).to_le_bytes());
                        } else {
                            buf.push(4);
                            buf.extend_from_slice(&n.to_le_bytes());
                        }
                    }
                    Value::BigInt(n) => {
                        buf.push(8);
                        buf.extend_from_slice(&n.to_le_bytes());
                    }
                    _ => buf.push(0), // NULL fallback
                }
            }
            TDS_TYPE_FLTN => {
                match val {
                    Value::Null => buf.push(0),
                    Value::Float(f) => {
                        buf.push(8);
                        buf.extend_from_slice(&f.to_le_bytes());
                    }
                    _ => buf.push(0),
                }
            }
            TDS_TYPE_BITN => {
                match val {
                    Value::Null => buf.push(0),
                    Value::Boolean(b) => {
                        buf.push(1);
                        buf.push(if *b { 1 } else { 0 });
                    }
                    _ => buf.push(0),
                }
            }
            // ----- NVARCHAR: 2-byte length prefix -----
            _ => {
                match val {
                    Value::Null => buf.extend_from_slice(&0xFFFFu16.to_le_bytes()),
                    _ => Self::encode_value_nvarchar(buf, &val.to_string()),
                }
            }
        }
    }

    #[inline]
    fn encode_value_nvarchar(buf: &mut Vec<u8>, s: &str) {
        let utf16: Vec<u16> = s.encode_utf16().collect();
        let bytes: Vec<u8> = utf16.iter().flat_map(|w| w.to_le_bytes()).collect();
        buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&bytes);
    }
}

impl Drop for TdsConnection {
    fn drop(&mut self) {
        if let Some(txn_id) = self.current_txn_id.take() {
            self.db.abort_transaction(txn_id);
        }
        // Clean up temp tables created by this connection
        self.db.cleanup_temp_tables(self.conn_id);
    }
}
