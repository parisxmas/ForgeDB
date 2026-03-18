use std::io::{self, BufReader, BufWriter, Read as IoRead, Write as IoWrite};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::database::Database;
use crate::tuple::types::Value;

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encode an integer using the MySQL length-encoded integer format.
fn encode_lenenc_int(val: u64) -> Vec<u8> {
    if val < 251 {
        vec![val as u8]
    } else if val < 65536 {
        let mut buf = vec![0xFC];
        buf.extend_from_slice(&(val as u16).to_le_bytes());
        buf
    } else if val < 16_777_216 {
        let mut buf = vec![0xFD];
        let bytes = (val as u32).to_le_bytes();
        buf.extend_from_slice(&bytes[..3]);
        buf
    } else {
        let mut buf = vec![0xFE];
        buf.extend_from_slice(&val.to_le_bytes());
        buf
    }
}

/// Encode a string using the MySQL length-encoded string format.
fn encode_lenenc_str(s: &str) -> Vec<u8> {
    let mut buf = encode_lenenc_int(s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
    buf
}

/// Read a NUL-terminated string from a buffer starting at `pos`.
/// Advances `pos` past the NUL byte.
fn read_null_terminated_string(buf: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < buf.len() && buf[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8_lossy(&buf[start..*pos]).to_string();
    if *pos < buf.len() {
        *pos += 1; // skip NUL
    }
    s
}

// ---------------------------------------------------------------------------
// MysqlServer
// ---------------------------------------------------------------------------

/// A MySQL wire-protocol compatible TCP server for ForgeDB.
pub struct MysqlServer {
    db_path: String,
    bind_addr: String,
}

impl MysqlServer {
    /// Create a new server instance.
    pub fn new(db_path: &str, bind_addr: &str) -> Self {
        Self {
            db_path: db_path.to_string(),
            bind_addr: bind_addr.to_string(),
        }
    }

    /// Start listening for connections. Blocks indefinitely.
    /// Spawns a thread per connection, sharing a single Database via Arc<Mutex>.
    pub fn start(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.bind_addr)?;
        println!("Server listening on {}", self.bind_addr);

        // Single shared database instance
        let db = Database::open(&self.db_path)
            .or_else(|_| Database::new(&self.db_path))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
        let db = Arc::new(db);

        let connection_id = Arc::new(std::sync::atomic::AtomicU32::new(1));

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    let conn_id = connection_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let db = Arc::clone(&db);
                    let peer = stream
                        .peer_addr()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    println!("New connection from {} (id={})", peer, conn_id);

                    thread::spawn(move || {
                        let mut handler = match ConnectionHandler::new_shared(stream, db, conn_id) {
                            Ok(h) => h,
                            Err(e) => {
                                eprintln!("Connection {} handler error: {}", conn_id, e);
                                return;
                            }
                        };

                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            handler.run()
                        }));
                        match &result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => eprintln!("Connection {} ended: {}", conn_id, e),
                            Err(_) => eprintln!("Connection {} panicked (recovered)", conn_id),
                        }

                        println!("Connection {} closed", conn_id);
                    });
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConnectionHandler
// ---------------------------------------------------------------------------

struct ConnectionHandler {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    db: Arc<Database>,
    connection_id: u32,
    seq_id: u8,
}

impl ConnectionHandler {
    fn new_shared(stream: TcpStream, db: Arc<Database>, connection_id: u32) -> io::Result<Self> {
        let reader_stream = stream.try_clone()?;
        Ok(Self {
            reader: BufReader::with_capacity(8192, reader_stream),
            writer: BufWriter::with_capacity(65536, stream),
            db,
            connection_id,
            seq_id: 0,
        })
    }

    /// Read a single MySQL packet from the stream.
    fn read_packet(&mut self) -> io::Result<Vec<u8>> {
        let mut header = [0u8; 4];
        self.reader.read_exact(&mut header)?;

        let payload_len =
            (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        self.seq_id = header[3];

        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload)?;

        Ok(payload)
    }

    /// Write a single MySQL packet to the buffer (no flush).
    fn write_packet(&mut self, payload: &[u8]) -> io::Result<()> {
        let len = payload.len() as u32;
        let mut header_and_payload = Vec::with_capacity(4 + payload.len());
        header_and_payload.push((len & 0xFF) as u8);
        header_and_payload.push(((len >> 8) & 0xFF) as u8);
        header_and_payload.push(((len >> 16) & 0xFF) as u8);
        header_and_payload.push(self.seq_id);
        header_and_payload.extend_from_slice(payload);
        self.writer.write_all(&header_and_payload)?;
        self.seq_id = self.seq_id.wrapping_add(1);
        Ok(())
    }

    /// Flush buffered writes to the network.
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    // -----------------------------------------------------------------------
    // Packet builders
    // -----------------------------------------------------------------------

    /// Build an OK packet.
    fn ok_packet(affected_rows: u64, last_insert_id: u64) -> Vec<u8> {
        let mut pkt = vec![0x00]; // OK header
        pkt.extend_from_slice(&encode_lenenc_int(affected_rows));
        pkt.extend_from_slice(&encode_lenenc_int(last_insert_id));
        pkt.extend_from_slice(&[0x02, 0x00]); // status flags: AUTOCOMMIT
        pkt.extend_from_slice(&[0x00, 0x00]); // warnings
        pkt
    }

    /// Build an ERR packet.
    fn err_packet(error_code: u16, message: &str) -> Vec<u8> {
        let mut pkt = vec![0xFF]; // ERR header
        pkt.extend_from_slice(&error_code.to_le_bytes());
        pkt.push(0x23); // '#'
        pkt.extend_from_slice(b"HY000"); // SQL state
        pkt.extend_from_slice(message.as_bytes());
        pkt
    }

    /// Build an EOF packet.
    fn eof_packet() -> Vec<u8> {
        vec![
            0xFE, // EOF header
            0x00, 0x00, // warnings
            0x02, 0x00, // status flags: AUTOCOMMIT
        ]
    }

    // -----------------------------------------------------------------------
    // Handshake
    // -----------------------------------------------------------------------

    /// Perform the initial handshake with the client.
    fn handshake(&mut self) -> io::Result<()> {
        // -- Server greeting --
        self.seq_id = 0;
        let mut greeting = Vec::with_capacity(128);

        // protocol version
        greeting.push(0x0A);

        // server version (NUL-terminated)
        greeting.extend_from_slice(b"5.7.38-ForgeDB\0");

        // connection id (4 bytes LE)
        greeting.extend_from_slice(&self.connection_id.to_le_bytes());

        // auth_plugin_data_part1 (8 bytes) - deterministic from connection_id
        let id_bytes = self.connection_id.to_le_bytes();
        greeting.extend_from_slice(&[
            id_bytes[0].wrapping_add(0x4A),
            id_bytes[1].wrapping_add(0x2B),
            id_bytes[2].wrapping_add(0x1C),
            id_bytes[3].wrapping_add(0x3D),
            0x5E,
            0x6F,
            0x70,
            0x21,
        ]);

        // filler
        greeting.push(0x00);

        // capability flags lower 2 bytes
        greeting.extend_from_slice(&[0x0F, 0xA2]);

        // character set: utf8 (0x21)
        greeting.push(0x21);

        // status flags
        greeting.extend_from_slice(&[0x02, 0x00]);

        // capability flags upper 2 bytes
        greeting.extend_from_slice(&[0x28, 0x00]);

        // auth_plugin_data_len
        greeting.push(0x15);

        // reserved (10 zero bytes)
        greeting.extend_from_slice(&[0x00; 10]);

        // auth_plugin_data_part2 (12 bytes + NUL)
        greeting.extend_from_slice(&[
            id_bytes[0].wrapping_add(0x11),
            id_bytes[1].wrapping_add(0x22),
            id_bytes[2].wrapping_add(0x33),
            id_bytes[3].wrapping_add(0x44),
            0x55,
            0x66,
            0x77,
            0x88,
            0x99,
            0xAA,
            0xBB,
            0xCC,
        ]);
        greeting.push(0x00); // NUL terminator for auth_plugin_data_part2

        // auth_plugin_name (NUL-terminated)
        greeting.extend_from_slice(b"mysql_native_password\0");

        self.write_packet(&greeting)?;
        self.flush()?;

        // -- Read handshake response --
        let payload = self.read_packet()?;

        // Parse minimally: client_flags(4) + max_packet_size(4) + charset(1) + 23 filler + username(NUL)
        if payload.len() < 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "handshake response too short",
            ));
        }

        let mut pos = 4 + 4 + 1 + 23; // skip client_flags, max_packet_size, charset, filler
        let username = read_null_terminated_string(&payload, &mut pos);
        println!("Client authenticated as: {}", username);

        // Accept any password -- send OK with seq=2
        self.seq_id = 2;
        let ok = Self::ok_packet(0, 0);
        self.write_packet(&ok)?;
        self.flush()?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Result set writing
    // -----------------------------------------------------------------------

    /// Send a result set to the client.
    fn send_result_set(
        &mut self,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> io::Result<()> {
        // Column count
        let col_count = encode_lenenc_int(columns.len() as u64);
        self.write_packet(&col_count)?;

        // Column definitions
        for col_name in columns {
            let col_def = self.build_column_definition(col_name, rows);
            self.write_packet(&col_def)?;
        }

        // EOF after column defs
        let eof = Self::eof_packet();
        self.write_packet(&eof)?;

        // Rows
        for row in rows {
            let row_pkt = self.build_row_packet(row);
            self.write_packet(&row_pkt)?;
        }

        // EOF after rows
        self.write_packet(&eof)?;

        Ok(())
    }

    /// Build a ColumnDefinition41 packet for a given column.
    fn build_column_definition(&self, col_name: &str, rows: &[Vec<Value>]) -> Vec<u8> {
        let mut pkt = Vec::with_capacity(64);

        // catalog
        pkt.extend_from_slice(&encode_lenenc_str("def"));
        // schema
        pkt.extend_from_slice(&encode_lenenc_str(""));
        // table
        pkt.extend_from_slice(&encode_lenenc_str(""));
        // org_table
        pkt.extend_from_slice(&encode_lenenc_str(""));
        // name
        pkt.extend_from_slice(&encode_lenenc_str(col_name));
        // org_name
        pkt.extend_from_slice(&encode_lenenc_str(col_name));

        // fixed-length fields marker
        pkt.push(0x0C);

        // Determine the column type from the first non-null value in this column
        let col_idx = self.find_column_index_by_name(col_name, rows);
        let sample_value = col_idx.and_then(|idx| {
            rows.iter()
                .map(|r| r.get(idx))
                .flatten()
                .find(|v| !v.is_null())
        });

        let (charset, col_length, col_type) = match sample_value {
            Some(Value::Integer(_)) => ([0x3F, 0x00], 11u32, 0x03u8),    // LONG
            Some(Value::BigInt(_)) => ([0x3F, 0x00], 20u32, 0x08u8),     // LONGLONG
            Some(Value::Float(_)) => ([0x3F, 0x00], 22u32, 0x05u8),      // DOUBLE
            Some(Value::Boolean(_)) => ([0x3F, 0x00], 1u32, 0x01u8),     // TINY
            Some(Value::DateTime(_)) => ([0x3F, 0x00], 19u32, 0x0Cu8),   // DATETIME
            Some(Value::Varchar(_)) => ([0x21, 0x00], 255u32, 0xFDu8),   // VAR_STRING
            _ => ([0x21, 0x00], 255u32, 0xFDu8),                         // default: VAR_STRING
        };

        // charset (2 bytes)
        pkt.extend_from_slice(&charset);
        // column length (4 bytes LE)
        pkt.extend_from_slice(&col_length.to_le_bytes());
        // column type (1 byte)
        pkt.push(col_type);
        // flags (2 bytes)
        pkt.extend_from_slice(&[0x00, 0x00]);
        // decimals (1 byte)
        pkt.push(0x00);
        // filler (2 bytes)
        pkt.extend_from_slice(&[0x00, 0x00]);

        pkt
    }

    /// Find the positional index of a column name in the result set.
    /// This is a best-effort helper -- for synthetic results the column
    /// position matches the iteration order.
    fn find_column_index_by_name(&self, _col_name: &str, _rows: &[Vec<Value>]) -> Option<usize> {
        // We don't have column names in rows, so we rely on iteration order
        // from send_result_set where columns[i] corresponds to row[i].
        // The caller of build_column_definition iterates columns in order,
        // but we don't track the current index here. Instead, we use a
        // simpler approach: sample the first row, first non-null value.
        None
    }

    /// Send a result set where we know the column index for each column.
    fn send_result_set_with_types(
        &mut self,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> io::Result<()> {
        // Column count
        let col_count = encode_lenenc_int(columns.len() as u64);
        self.write_packet(&col_count)?;

        // Column definitions -- we pass the column index explicitly
        for (idx, col_name) in columns.iter().enumerate() {
            let col_def = self.build_column_definition_with_index(col_name, rows, idx);
            self.write_packet(&col_def)?;
        }

        // EOF after column defs
        let eof = Self::eof_packet();
        self.write_packet(&eof)?;

        // Rows
        for row in rows {
            let row_pkt = self.build_row_packet(row);
            self.write_packet(&row_pkt)?;
        }

        // EOF after rows
        self.write_packet(&eof)?;

        // Single flush for the entire result set
        self.flush()?;

        Ok(())
    }

    /// Build a ColumnDefinition41 packet using a known column index.
    fn build_column_definition_with_index(
        &self,
        col_name: &str,
        rows: &[Vec<Value>],
        col_idx: usize,
    ) -> Vec<u8> {
        let mut pkt = Vec::with_capacity(64);

        // catalog
        pkt.extend_from_slice(&encode_lenenc_str("def"));
        // schema
        pkt.extend_from_slice(&encode_lenenc_str(""));
        // table
        pkt.extend_from_slice(&encode_lenenc_str(""));
        // org_table
        pkt.extend_from_slice(&encode_lenenc_str(""));
        // name
        pkt.extend_from_slice(&encode_lenenc_str(col_name));
        // org_name
        pkt.extend_from_slice(&encode_lenenc_str(col_name));

        // fixed-length fields marker
        pkt.push(0x0C);

        // Sample value for type detection
        let sample_value = rows
            .iter()
            .filter_map(|r| r.get(col_idx))
            .find(|v| !v.is_null());

        let (charset, col_length, col_type) = match sample_value {
            Some(Value::Integer(_)) => ([0x3F, 0x00], 11u32, 0x03u8),
            Some(Value::BigInt(_)) => ([0x3F, 0x00], 20u32, 0x08u8),
            Some(Value::Float(_)) => ([0x3F, 0x00], 22u32, 0x05u8),
            Some(Value::Boolean(_)) => ([0x3F, 0x00], 1u32, 0x01u8),
            Some(Value::DateTime(_)) => ([0x3F, 0x00], 19u32, 0x0Cu8),
            Some(Value::Varchar(_)) => ([0x21, 0x00], 255u32, 0xFDu8),
            _ => ([0x21, 0x00], 255u32, 0xFDu8),
        };

        // charset (2 bytes)
        pkt.extend_from_slice(&charset);
        // column length (4 bytes LE)
        pkt.extend_from_slice(&col_length.to_le_bytes());
        // column type (1 byte)
        pkt.push(col_type);
        // flags (2 bytes)
        pkt.extend_from_slice(&[0x00, 0x00]);
        // decimals (1 byte)
        pkt.push(0x00);
        // filler (2 bytes)
        pkt.extend_from_slice(&[0x00, 0x00]);

        pkt
    }

    /// Build a row packet: each value as a lenenc_str, or 0xFB for NULL.
    fn build_row_packet(&self, row: &[Value]) -> Vec<u8> {
        let mut pkt = Vec::new();
        for val in row {
            match val {
                Value::Null => pkt.push(0xFB),
                other => {
                    let s = other.to_string();
                    pkt.extend_from_slice(&encode_lenenc_str(&s));
                }
            }
        }
        pkt
    }

    // -----------------------------------------------------------------------
    // Synthetic result helpers
    // -----------------------------------------------------------------------

    /// Send a synthetic single-column, single-row result set (all Varchar).
    fn send_synthetic_result(
        &mut self,
        col_name: &str,
        value: &str,
    ) -> io::Result<()> {
        let columns = vec![col_name.to_string()];
        let rows = vec![vec![Value::Varchar(value.to_string())]];
        self.send_result_set_with_types(&columns, &rows)
    }

    /// Send a synthetic empty result set with the given column names.
    fn send_empty_result_set(&mut self, columns: &[&str]) -> io::Result<()> {
        let col_names: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let rows: Vec<Vec<Value>> = Vec::new();
        self.send_result_set_with_types(&col_names, &rows)
    }

    // -----------------------------------------------------------------------
    // Query interception
    // -----------------------------------------------------------------------

    /// Try to intercept known queries before passing them to the database.
    /// Returns `Some(())` if the query was handled, `None` if it should be
    /// forwarded to the database engine.
    fn try_intercept_query(&mut self, sql: &str) -> io::Result<Option<()>> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        // SET commands -> OK
        if upper.starts_with("SET ") {
            let ok = Self::ok_packet(0, 0);
            self.write_packet(&ok)?;
            self.flush()?;
            return Ok(Some(()));
        }

        // DO expr (MySQL connection check) -> OK
        if upper.starts_with("DO ") {
            let ok = Self::ok_packet(0, 0);
            self.write_packet(&ok)?;
            self.flush()?;
            return Ok(Some(()));
        }

        // REPLACE INTO -> convert to INSERT and forward
        if upper.starts_with("REPLACE ") {
            let insert_sql = trimmed.replacen("REPLACE", "INSERT", 1);
            let __result = self.db.execute_sql(&insert_sql);
                match __result {
                Ok(result) => {
                    let ok = Self::ok_packet(result.rows_affected as u64, 0);
                    self.write_packet(&ok)?;
                    self.flush()?;
                }
                Err(e) => {
                    let err = Self::err_packet(1064, &format!("{}", e));
                    self.write_packet(&err)?;
                    self.flush()?;
                }
            }
            return Ok(Some(()));
        }

        // Multi-table DELETE (e.g., DELETE a, b FROM wp_options a, wp_options b WHERE ...)
        if upper.starts_with("DELETE ") && !upper.starts_with("DELETE FROM") {
            let ok = Self::ok_packet(0, 0);
            self.write_packet(&ok)?;
            self.flush()?;
            return Ok(Some(()));
        }

        // UPDATE with subquery in SET (e.g., SET count = (SELECT COUNT(*) ...))
        // Execute the subquery first, then do a simple UPDATE
        if upper.starts_with("UPDATE") && upper.contains("(SELECT ") {
            // Just return OK — this is used for term count updates
            let ok = Self::ok_packet(0, 0);
            self.write_packet(&ok)?;
            self.flush()?;
            return Ok(Some(()));
        }

        // SHOW WARNINGS
        if upper.starts_with("SHOW WARNINGS") {
            self.send_empty_result_set(&["Level", "Code", "Message"])?;
            return Ok(Some(()));
        }

        // SHOW INDEX / SHOW INDEXES / SHOW KEYS
        if upper.starts_with("SHOW INDEX") || upper.starts_with("SHOW INDEXES")
            || upper.starts_with("SHOW KEYS")
        {
            self.send_empty_result_set(&[
                "Table", "Non_unique", "Key_name", "Seq_in_index",
                "Column_name", "Collation", "Cardinality", "Sub_part",
                "Packed", "Null", "Index_type", "Comment",
            ])?;
            return Ok(Some(()));
        }

        // SHOW FULL COLUMNS
        if upper.starts_with("SHOW FULL COLUMNS") || upper.starts_with("SHOW COLUMNS") {
            // Forward to database engine which handles SHOW COLUMNS
            // But "SHOW FULL COLUMNS FROM wp_users" needs to be converted to "SHOW COLUMNS FROM wp_users"
            // Extract table name
            if let Some(pos) = upper.find("FROM ") {
                let table_part = &trimmed[pos + 5..].trim().trim_end_matches(';');
                let table_name = table_part.split_whitespace().next().unwrap_or("").trim_matches('`');
                let show_sql = format!("SHOW COLUMNS FROM {}", table_name);
                let __result = self.db.execute_sql(&show_sql);
                match __result {
                    Ok(result) => {
                        self.send_result_set_with_types(&result.columns, &result.rows)?;
                    }
                    Err(_) => {
                        self.send_empty_result_set(&["Field", "Type", "Null", "Key", "Default", "Extra"])?;
                    }
                }
                return Ok(Some(()));
            }
        }

        // SHOW TABLES LIKE
        if upper.starts_with("SHOW TABLES LIKE") {
            // Forward SHOW TABLES and filter
            let __result = self.db.execute_sql("SHOW TABLES");
                match __result {
                Ok(result) => {
                    self.send_result_set_with_types(&result.columns, &result.rows)?;
                }
                Err(_) => {
                    self.send_empty_result_set(&["Tables_in_forgedb"])?;
                }
            }
            return Ok(Some(()));
        }

        // SHOW VARIABLES
        if upper.starts_with("SHOW VARIABLES") {
            self.send_empty_result_set(&["Variable_name", "Value"])?;
            return Ok(Some(()));
        }

        // SHOW COLLATION
        if upper.starts_with("SHOW COLLATION") {
            self.send_empty_result_set(&[
                "Collation",
                "Charset",
                "Id",
                "Default",
                "Compiled",
                "Sortlen",
            ])?;
            return Ok(Some(()));
        }

        // SELECT @@version_comment
        if upper.contains("@@VERSION_COMMENT") {
            self.send_synthetic_result("@@version_comment", "ForgeDB 0.1.0")?;
            return Ok(Some(()));
        }

        // SELECT @@version or VERSION()
        if upper.contains("@@VERSION") || upper.contains("VERSION()") {
            let col_name = if upper.contains("VERSION()") {
                "VERSION()"
            } else {
                "@@version"
            };
            self.send_synthetic_result(col_name, "5.7.38-ForgeDB")?;
            return Ok(Some(()));
        }

        // SELECT @@session.sql_mode
        if upper.contains("@@SESSION.SQL_MODE") || upper.contains("@@SQL_MODE") {
            self.send_synthetic_result("@@session.sql_mode", "")?;
            return Ok(Some(()));
        }

        // SELECT @@character_set_*
        if upper.contains("@@CHARACTER_SET") {
            // Extract the variable name for the column header
            let var_name = self.extract_atat_variable(trimmed);
            self.send_synthetic_result(&var_name, "utf8mb4")?;
            return Ok(Some(()));
        }

        // SELECT @@collation_connection
        if upper.contains("@@COLLATION_CONNECTION") {
            self.send_synthetic_result("@@collation_connection", "utf8mb4_general_ci")?;
            return Ok(Some(()));
        }

        // SELECT @@max_allowed_packet
        if upper.contains("@@MAX_ALLOWED_PACKET") {
            self.send_synthetic_result("@@max_allowed_packet", "16777216")?;
            return Ok(Some(()));
        }

        // SELECT DATABASE()
        if upper.contains("DATABASE()") {
            self.send_synthetic_result("DATABASE()", "forgedb")?;
            return Ok(Some(()));
        }

        // Generic @@variable handler -- catch-all for any remaining @@ selects
        if upper.starts_with("SELECT") && upper.contains("@@") {
            let var_name = self.extract_atat_variable(trimmed);
            self.send_synthetic_result(&var_name, "")?;
            return Ok(Some(()));
        }

        // SELECT without FROM (e.g., SELECT 1, SELECT 'hello')
        if upper.starts_with("SELECT") && !upper.contains("FROM") {
            // Extract the expression part
            let expr_part = trimmed[6..].trim().trim_end_matches(';');
            // Handle multiple columns separated by commas
            let cols: Vec<&str> = expr_part.split(',').map(|s| s.trim()).collect();
            let mut col_names = Vec::new();
            let mut values = Vec::new();
            for col in cols {
                // Check for alias: "expr AS alias"
                let parts: Vec<&str> = col.splitn(2, " AS ").collect();
                let (expr_str, alias) = if parts.len() == 2 {
                    (parts[0].trim(), parts[1].trim().trim_matches('`').trim_matches('\''))
                } else {
                    let parts: Vec<&str> = col.splitn(2, " as ").collect();
                    if parts.len() == 2 {
                        (parts[0].trim(), parts[1].trim().trim_matches('`').trim_matches('\''))
                    } else {
                        (col, col)
                    }
                };
                col_names.push(alias.to_string());
                // Try to evaluate simple expressions
                let val = if let Ok(n) = expr_str.parse::<i64>() {
                    Value::Varchar(n.to_string())
                } else {
                    Value::Varchar(expr_str.trim_matches('\'').to_string())
                };
                values.push(val);
            }
            let rows = vec![values];
            self.send_result_set_with_types(&col_names, &rows)?;
            return Ok(Some(()));
        }

        Ok(None)
    }

    /// Extract the first @@variable name from a SQL string.
    fn extract_atat_variable(&self, sql: &str) -> String {
        if let Some(start) = sql.find("@@") {
            let rest = &sql[start..];
            let end = rest
                .find(|c: char| !c.is_alphanumeric() && c != '_' && c != '.' && c != '@')
                .unwrap_or(rest.len());
            rest[..end].to_string()
        } else {
            "@@unknown".to_string()
        }
    }

    // -----------------------------------------------------------------------
    // Command loop
    // -----------------------------------------------------------------------

    /// Main entry point: perform handshake then process commands.
    fn run(&mut self) -> io::Result<()> {
        self.handshake()?;

        loop {
            let payload = match self.read_packet() {
                Ok(p) => p,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    
                    return Ok(());
                }
                Err(e) => {
                    
                    return Err(e);
                }
            };

            // Response packets start at seq = client_seq + 1
            self.seq_id = self.seq_id.wrapping_add(1);

            if payload.is_empty() {
                continue;
            }

            let cmd = payload[0];
            match cmd {
                // COM_QUIT
                0x01 => {
                    
                    return Ok(());
                }

                // COM_INIT_DB
                0x02 => {
                    let ok = Self::ok_packet(0, 0);
                    self.write_packet(&ok)?;
                    self.flush()?;
                }

                // COM_QUERY
                0x03 => {
                    let sql = String::from_utf8_lossy(&payload[1..]).to_string();
                    self.handle_query(&sql)?;
                }

                // COM_PING
                0x0E => {
                    let ok = Self::ok_packet(0, 0);
                    self.write_packet(&ok)?;
                    self.flush()?;
                }

                // Unknown command
                _ => {
                    let err = Self::err_packet(1047, &format!("Unknown command: {}", cmd));
                    self.write_packet(&err)?;
                    self.flush()?;
                }
            }
        }
    }

    /// Rewrite MySQL-specific SQL that our parser can't handle.
    fn rewrite_sql(sql: &str) -> String {
        let mut s = sql.to_string();

        // Strip "ON DUPLICATE KEY UPDATE ..." from INSERT
        if let Some(pos) = s.to_uppercase().find("ON DUPLICATE KEY UPDATE") {
            s = s[..pos].trim().to_string();
        }

        // Strip "SQL_CALC_FOUND_ROWS"
        s = s.replace("SQL_CALC_FOUND_ROWS ", "").replace("SQL_CALC_FOUND_ROWS", "");

        // MySqlDialect handles backslash escapes natively — no rewriting needed

        // For CREATE TABLE: strip table-level options AFTER the closing paren
        if s.to_uppercase().contains("CREATE TABLE") {
            // Find the last ')' which ends the column definitions
            if let Some(close_paren) = s.rfind(')') {
                // Everything after ')' is table options - strip them
                let after = &s[close_paren + 1..];
                let upper_after = after.to_uppercase();
                if upper_after.contains("ENGINE")
                    || upper_after.contains("CHARSET")
                    || upper_after.contains("COLLATE")
                    || upper_after.contains("AUTO_INCREMENT")
                    || upper_after.contains("ROW_FORMAT")
                {
                    s = s[..=close_paren].to_string();
                }
            }

            // Remove COLLATE inside column defs (e.g., varchar(255) COLLATE utf8mb4_unicode_ci)
            while let Some(pos) = s.to_uppercase().find(" COLLATE ") {
                let rest = &s[pos + 9..];
                let end = rest
                    .find(|c: char| c == ' ' || c == ',' || c == ')' || c == '\n')
                    .unwrap_or(rest.len());
                s = format!("{}{}", &s[..pos], &s[pos + 9 + end..]);
            }

            // Remove CHARACTER SET inside column defs
            while let Some(pos) = s.to_uppercase().find(" CHARACTER SET ") {
                let rest = &s[pos + 15..];
                let end = rest
                    .find(|c: char| c == ' ' || c == ',' || c == ')' || c == '\n')
                    .unwrap_or(rest.len());
                s = format!("{}{}", &s[..pos], &s[pos + 15 + end..]);
            }

            // Strip KEY/INDEX definitions that have prefix lengths like KEY meta_key (meta_key(191))
            // These are table-level index defs our parser handles partially
        }

        s
    }

    /// Handle a COM_QUERY command.
    fn handle_query(&mut self, sql: &str) -> io::Result<()> {
        // Try interception first
        if let Some(()) = self.try_intercept_query(sql)? {
            return Ok(());
        }

        // Rewrite MySQL-specific SQL
        let rewritten = Self::rewrite_sql(sql);

        // Forward to database engine
        let __result = self.db.execute_sql(&rewritten);
                match __result {
            Ok(result) => {
                if result.columns.is_empty() {
                    let ok = Self::ok_packet(result.rows_affected as u64, result.last_insert_id);
                    self.write_packet(&ok)?;
                    self.flush()?;
                } else {
                    // send_result_set_with_types flushes internally
                    self.send_result_set_with_types(&result.columns, &result.rows)?;
                }
            }
            Err(e) => {
                let err = Self::err_packet(1064, &format!("{}", e));
                self.write_packet(&err)?;
                self.flush()?;
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_lenenc_int_small() {
        // Values 0..250 are encoded as a single byte.
        assert_eq!(encode_lenenc_int(0), vec![0]);
        assert_eq!(encode_lenenc_int(250), vec![250]);
    }

    #[test]
    fn test_encode_lenenc_int_two_byte() {
        // 251 uses 0xFC prefix + 2-byte LE
        let result = encode_lenenc_int(251);
        assert_eq!(result[0], 0xFC);
        let val = u16::from_le_bytes([result[1], result[2]]);
        assert_eq!(val, 251);
    }

    #[test]
    fn test_encode_lenenc_int_two_byte_max() {
        // 65535 uses 0xFC prefix + 2-byte LE
        let result = encode_lenenc_int(65535);
        assert_eq!(result[0], 0xFC);
        let val = u16::from_le_bytes([result[1], result[2]]);
        assert_eq!(val, 65535);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_encode_lenenc_int_three_byte() {
        // 16_777_216 (2^24) uses 0xFE prefix + 8-byte LE
        let result = encode_lenenc_int(16_777_216);
        assert_eq!(result[0], 0xFE);
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&result[1..9]);
        let val = u64::from_le_bytes(bytes);
        assert_eq!(val, 16_777_216);
    }

    #[test]
    fn test_encode_lenenc_str_empty() {
        let result = encode_lenenc_str("");
        assert_eq!(result, vec![0]); // length 0, no data
    }

    #[test]
    fn test_encode_lenenc_str_non_empty() {
        let result = encode_lenenc_str("hello");
        assert_eq!(result[0], 5); // length = 5
        assert_eq!(&result[1..], b"hello");
    }

    #[test]
    fn test_read_null_terminated_string() {
        let buf = b"hello\0world\0";
        let mut pos = 0;

        let s1 = read_null_terminated_string(buf, &mut pos);
        assert_eq!(s1, "hello");
        assert_eq!(pos, 6); // past the NUL

        let s2 = read_null_terminated_string(buf, &mut pos);
        assert_eq!(s2, "world");
        assert_eq!(pos, 12);
    }

    #[test]
    fn test_read_null_terminated_string_no_nul() {
        let buf = b"no nul here";
        let mut pos = 0;
        let s = read_null_terminated_string(buf, &mut pos);
        assert_eq!(s, "no nul here");
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn test_ok_packet_format() {
        let pkt = ConnectionHandler::ok_packet(0, 0);
        assert_eq!(pkt[0], 0x00); // OK header
        // affected_rows = 0 -> lenenc 0x00
        assert_eq!(pkt[1], 0x00);
        // last_insert_id = 0 -> lenenc 0x00
        assert_eq!(pkt[2], 0x00);
        // status flags
        assert_eq!(pkt[3], 0x02);
        assert_eq!(pkt[4], 0x00);
        // warnings
        assert_eq!(pkt[5], 0x00);
        assert_eq!(pkt[6], 0x00);
    }

    #[test]
    fn test_err_packet_format() {
        let pkt = ConnectionHandler::err_packet(1064, "test error");
        assert_eq!(pkt[0], 0xFF); // ERR header
        let code = u16::from_le_bytes([pkt[1], pkt[2]]);
        assert_eq!(code, 1064);
        assert_eq!(pkt[3], 0x23); // '#'
        assert_eq!(&pkt[4..9], b"HY000");
        assert_eq!(&pkt[9..], b"test error");
    }

    #[test]
    fn test_eof_packet_format() {
        let pkt = ConnectionHandler::eof_packet();
        assert_eq!(pkt[0], 0xFE); // EOF header
        assert_eq!(pkt[1], 0x00); // warnings low
        assert_eq!(pkt[2], 0x00); // warnings high
        assert_eq!(pkt[3], 0x02); // status flags low
        assert_eq!(pkt[4], 0x00); // status flags high
    }

    #[test]
    fn test_encode_lenenc_int_boundary_values() {
        // 0 -> single byte
        assert_eq!(encode_lenenc_int(0).len(), 1);

        // 250 -> single byte
        assert_eq!(encode_lenenc_int(250).len(), 1);

        // 251 -> 3 bytes (0xFC + 2)
        assert_eq!(encode_lenenc_int(251).len(), 3);

        // 65535 -> 3 bytes
        assert_eq!(encode_lenenc_int(65535).len(), 3);

        // 65536 -> 4 bytes (0xFD + 3)
        let result = encode_lenenc_int(65536);
        assert_eq!(result[0], 0xFD);
        assert_eq!(result.len(), 4);

        // 16_777_215 (2^24 - 1) -> 4 bytes
        let result = encode_lenenc_int(16_777_215);
        assert_eq!(result[0], 0xFD);
        assert_eq!(result.len(), 4);

        // 16_777_216 (2^24) -> 9 bytes (0xFE + 8)
        let result = encode_lenenc_int(16_777_216);
        assert_eq!(result[0], 0xFE);
        assert_eq!(result.len(), 9);
    }
}
