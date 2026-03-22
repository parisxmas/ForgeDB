//! ForgeWire — ForgeDB's native binary wire protocol.
//!
//! Designed for maximum throughput: raw UTF-8 SQL, little-endian binary values,
//! minimal framing, pipelining support.
//!
//! ## Frame format
//! ```text
//! [msg_type: u8] [length: u32 LE] [payload: length bytes]
//! ```
//! Total header: 5 bytes (vs TDS 8, PG 5+type overhead)
//!
//! ## Message types
//!
//! Client → Server:
//!   0x01 Query      — UTF-8 SQL text
//!   0x02 Prepare    — UTF-8 SQL template, returns handle
//!   0x03 Execute    — handle(u32) + param values
//!   0x04 Close      — close prepared statement handle
//!   0x05 Ping       — keepalive
//!   0xFF Disconnect  — graceful close
//!
//! Server → Client:
//!   0x10 RowHeader  — column count(u16) + [col_name_len(u16) + UTF-8 name + type_id(u8)] per col
//!   0x11 Row        — [value per column: type-specific encoding]
//!   0x12 Done       — rows_affected(u64)
//!   0x13 Error      — error_len(u16) + UTF-8 message
//!   0x14 PrepareOk  — handle(u32)
//!   0x15 Pong       — response to Ping
//!
//! ## Value encoding (in Row messages)
//!   Tag byte + data:
//!     0x00 NULL       — no data
//!     0x01 Int32      — 4 bytes LE
//!     0x02 Int64      — 8 bytes LE
//!     0x03 Float64    — 8 bytes LE
//!     0x04 Bool       — 1 byte (0/1)
//!     0x05 String     — len(u32 LE) + UTF-8 bytes
//!     0x06 Blob       — len(u32 LE) + raw bytes

use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read as IoRead, Write as IoWrite};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::database::Database;
use crate::tuple::types::Value;

// Message types: client → server
const MSG_QUERY: u8 = 0x01;
const MSG_PREPARE: u8 = 0x02;
const MSG_EXECUTE: u8 = 0x03;
const MSG_CLOSE_STMT: u8 = 0x04;
const MSG_PING: u8 = 0x05;
const MSG_BATCH_INSERT: u8 = 0x06;
const MSG_DISCONNECT: u8 = 0xFF;

// Message types: server → client
const MSG_ROW_HEADER: u8 = 0x10;
const MSG_ROW: u8 = 0x11;
const MSG_DONE: u8 = 0x12;
const MSG_ERROR: u8 = 0x13;
const MSG_PREPARE_OK: u8 = 0x14;
const MSG_PONG: u8 = 0x15;

// Value tags
const VAL_NULL: u8 = 0x00;
const VAL_INT32: u8 = 0x01;
const VAL_INT64: u8 = 0x02;
const VAL_FLOAT64: u8 = 0x03;
const VAL_BOOL: u8 = 0x04;
const VAL_STRING: u8 = 0x05;
const VAL_BLOB: u8 = 0x06;

// ---------------------------------------------------------------------------
// ForgeWireServer
// ---------------------------------------------------------------------------

pub struct ForgeWireServer {
    db_path: String,
    bind_addr: String,
}

impl ForgeWireServer {
    pub fn new(db_path: &str, bind_addr: &str) -> Self {
        Self { db_path: db_path.to_string(), bind_addr: bind_addr.to_string() }
    }

    pub fn start(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.bind_addr)?;
        println!("ForgeWire server listening on {}", self.bind_addr);

        let db = Database::open(&self.db_path)
            .or_else(|_| Database::new(&self.db_path))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
        let db = Arc::new(db);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    let db = Arc::clone(&db);
                    thread::spawn(move || {
                        let mut conn = match ForgeWireConn::new(stream, db) {
                            Ok(c) => c,
                            Err(_) => return,
                        };
                        let _ = conn.run();
                    });
                }
                Err(e) => eprintln!("ForgeWire accept error: {}", e),
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ForgeWireConn
// ---------------------------------------------------------------------------

struct ForgeWireConn {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    db: Arc<Database>,
    current_txn_id: Option<crate::common::TxnId>,
    prepared: HashMap<u32, String>,
    next_handle: u32,
}

impl ForgeWireConn {
    fn new(stream: TcpStream, db: Arc<Database>) -> io::Result<Self> {
        let r = stream.try_clone()?;
        Ok(Self {
            reader: BufReader::with_capacity(65536, r),
            writer: BufWriter::with_capacity(65536, stream),
            db,
            current_txn_id: None,
            prepared: HashMap::new(),
            next_handle: 1,
        })
    }

    // -- Frame I/O --

    fn read_msg(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let mut hdr = [0u8; 5];
        self.reader.read_exact(&mut hdr)?;
        let msg_type = hdr[0];
        let length = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut payload = vec![0u8; length];
        if length > 0 {
            self.reader.read_exact(&mut payload)?;
        }
        Ok((msg_type, payload))
    }

    fn write_msg(&mut self, msg_type: u8, payload: &[u8]) -> io::Result<()> {
        let mut hdr = [0u8; 5];
        hdr[0] = msg_type;
        let len = payload.len() as u32;
        hdr[1..5].copy_from_slice(&len.to_le_bytes());
        self.writer.write_all(&hdr)?;
        if !payload.is_empty() {
            self.writer.write_all(payload)?;
        }
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    // -- Main loop --

    fn run(&mut self) -> io::Result<()> {
        loop {
            let (msg_type, payload) = match self.read_msg() {
                Ok(m) => m,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            };
            match msg_type {
                MSG_QUERY => self.handle_query(&payload)?,
                MSG_PREPARE => self.handle_prepare(&payload)?,
                MSG_EXECUTE => self.handle_execute(&payload)?,
                MSG_CLOSE_STMT => self.handle_close(&payload)?,
                MSG_BATCH_INSERT => self.handle_batch_insert(&payload)?,
                MSG_PING => {
                    self.write_msg(MSG_PONG, &[])?;
                    self.flush()?;
                }
                MSG_DISCONNECT => return Ok(()),
                _ => {
                    self.send_error("unknown message type")?;
                }
            }
        }
    }

    // -- Query --

    fn handle_query(&mut self, payload: &[u8]) -> io::Result<()> {
        let sql = std::str::from_utf8(payload).unwrap_or("");

        // Try direct full-scan fast path — zero Value deserialization
        if let Some(wire_bytes) = self.try_direct_scan(sql) {
            self.writer.write_all(&wire_bytes)?;
            return self.flush();
        }

        // Try fast JOIN path — string-parsed, zero parser/planner overhead
        if let Some(wire_bytes) = self.try_fast_join_query(sql) {
            self.writer.write_all(&wire_bytes)?;
            return self.flush();
        }

        let result = self.db.execute_sql_session(sql, &mut self.current_txn_id);
        match result {
            Ok(r) => self.send_result(&r)?,
            Err(e) => self.send_error(&format!("{}", e))?,
        }
        self.flush()
    }

    /// Fast path for `SELECT * FROM table` — scan pages directly, encode raw tuples
    /// to ForgeWire binary without creating Value objects.
    fn try_direct_scan(&self, sql: &str) -> Option<Vec<u8>> {
        let trimmed = sql.trim();
        // Quick heuristic check without allocating uppercase string
        if trimmed.len() < 16 { return None; }
        let bytes = trimmed.as_bytes();
        // Check "SELECT * FROM " (case-insensitive, first 14 chars)
        if !(bytes[0].to_ascii_uppercase() == b'S'
            && bytes[6].to_ascii_uppercase() == b' '
            && bytes[7] == b'*'
            && bytes[8] == b' ') {
            return None;
        }
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT * FROM ") { return None; }
        // Must not contain WHERE, JOIN, ORDER, GROUP, HAVING, LIMIT, UNION
        if upper.contains("WHERE") || upper.contains("JOIN") || upper.contains("ORDER")
            || upper.contains("GROUP") || upper.contains("HAVING") || upper.contains("LIMIT")
            || upper.contains("UNION") {
            return None;
        }
        // Extract table name
        let rest = trimmed[14..].trim().trim_end_matches(';').trim();
        let table_name = rest.split_whitespace().next()?;
        if table_name.is_empty() { return None; }

        let catalog_guard = self.db.catalog_ref();
        let info = catalog_guard.get_table(table_name)?;
        let schema = &info.schema;
        let cbpm = self.db.cbpm_ref();
        let mvcc = info.mvcc_enabled;

        let mut out = Vec::with_capacity(64 * 1024);

        // ROW_HEADER
        let mut hdr = Vec::with_capacity(128);
        hdr.extend_from_slice(&(schema.columns.len() as u16).to_le_bytes());
        for col in &schema.columns {
            let nb = col.name.as_bytes();
            hdr.extend_from_slice(&(nb.len() as u16).to_le_bytes());
            hdr.extend_from_slice(nb);
            hdr.push(crate::executor::arena_join::datatype_to_wire_tag(&col.data_type));
        }
        Self::append_msg(&mut out, MSG_ROW_HEADER, &hdr);

        // Scan pages directly
        let mut row_count: u64 = 0;
        let mut current_pid = info.first_page_id;
        let col_info = crate::executor::arena_join::compute_column_info(schema);

        while current_pid.0 != crate::common::INVALID_PAGE_ID {
            if cbpm.fetch_page(current_pid).is_err() { break; }
            let guard = cbpm.read_page(current_pid).ok()?;
            let page = guard.data();
            let num_slots = crate::storage::heap_page::get_num_slots(page);

            for slot in 0..num_slots {
                if let Some((off, len)) = crate::storage::heap_page::get_tuple_slice(page, slot) {
                    let raw = &page[off..off + len];
                    let tuple_data = if mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                        let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                        if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                        &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                    } else { raw };

                    // Encode row directly from raw bytes
                    let row_start = out.len();
                    out.extend_from_slice(&[MSG_ROW, 0, 0, 0, 0]);
                    crate::executor::arena_join::encode_tuple_columns_raw(&mut out, tuple_data, schema, &col_info);
                    let payload_len = (out.len() - row_start - 5) as u32;
                    out[row_start + 1..row_start + 5].copy_from_slice(&payload_len.to_le_bytes());
                    row_count += 1;
                }
            }

            let next = crate::storage::heap_page::get_next_page_id(guard.data());
            drop(guard);
            let _ = cbpm.unpin_page(current_pid, false);
            current_pid = crate::common::PageId(next);
        }

        // DONE
        Self::append_msg(&mut out, MSG_DONE, &row_count.to_le_bytes());
        Some(out)
    }

    /// Fast path for queries with CASE expressions on integer columns.
    /// Evaluates CASE directly on raw tuple bytes — no Value allocation, no executor.
    fn try_fast_case_query(&self, sql: &str) -> Option<Vec<u8>> {
        let trimmed = sql.trim();
        if trimmed.len() < 20 { return None; }
        let upper = trimmed.to_uppercase();
        if !upper.contains("CASE") || !upper.contains("WHEN") { return None; }
        // Bail on complex queries
        if upper.contains("JOIN") || upper.contains("GROUP") || upper.contains("HAVING")
            || upper.contains("ORDER") || upper.contains("LIMIT") || upper.contains("UNION")
            || upper.contains("WHERE") { return None; }

        // Extract: SELECT col1, CASE WHEN col2>N THEN 'X' ... ELSE 'Y' END FROM table
        let from_pos = upper.find("FROM")?;
        let table_name = trimmed[from_pos + 4..].trim().trim_end_matches(';').trim()
            .split_whitespace().next()?;
        if table_name.is_empty() { return None; }

        // Parse CASE expression: extract WHEN conditions and THEN values
        let case_start = upper.find("CASE")?;
        let case_end = upper.find(" END")?;
        let case_body = &trimmed[case_start + 4..case_end].trim();

        // Parse WHEN clauses: WHEN col>N THEN 'X'
        let mut branches: Vec<(i64, &str)> = Vec::new(); // (threshold, result_string)
        let mut else_val: &str = "";

        let case_upper = case_body.to_uppercase();
        let mut pos = 0;
        while let Some(when_pos) = case_upper[pos..].find("WHEN ") {
            let abs_when = pos + when_pos + 5;
            let then_pos = case_upper[abs_when..].find("THEN ")?;
            let condition = case_body[abs_when - (case_start + 4)..abs_when - (case_start + 4) + then_pos].trim();

            // Parse condition: col>N or col<N
            let gt_pos = condition.find('>');
            if let Some(gp) = gt_pos {
                let threshold: i64 = condition[gp + 1..].trim().parse().ok()?;
                let then_start = abs_when + then_pos + 5;
                // Find next WHEN or ELSE or end
                let next_when = case_upper[then_start..].find("WHEN ").map(|p| then_start + p);
                let next_else = case_upper[then_start..].find("ELSE ").map(|p| then_start + p);
                let val_end = next_when.or(next_else).unwrap_or(case_upper.len());
                let val_str = case_body[then_start - (case_start + 4)..val_end - (case_start + 4)].trim()
                    .trim_matches('\'');
                branches.push((threshold, val_str));
                pos = val_end;
            } else {
                return None; // unsupported condition
            }
        }

        // Parse ELSE
        if let Some(else_pos) = case_upper.find("ELSE ") {
            else_val = case_body[else_pos + 5 - 0..].trim().trim_matches('\'');
        }

        if branches.is_empty() { return None; }

        // Find the CASE column (the column used in WHEN conditions)
        let first_condition = &case_body[..case_upper.find("THEN ")?];
        let first_when = first_condition.find(|c: char| c == '>' || c == '<')?;
        let case_col_name = first_condition[..first_when].trim().trim_start_matches("WHEN ").trim();
        // Strip table prefix
        let case_col_name = if let Some(dot) = case_col_name.rfind('.') { &case_col_name[dot + 1..] } else { case_col_name };

        // Find the other SELECT columns (before CASE)
        let select_part = trimmed[6..case_start].trim().trim_end_matches(',').trim();
        let prefix_cols: Vec<&str> = if select_part.is_empty() { vec![] }
        else { select_part.split(',').map(|c| {
            let c = c.trim();
            if let Some(dot) = c.rfind('.') { &c[dot + 1..] } else { c }
        }).collect() };

        let catalog_guard = self.db.catalog_ref();
        let info = catalog_guard.get_table(table_name)?;
        let schema = &info.schema;
        let cbpm = self.db.cbpm_ref();

        // Find column indices
        let (case_col_idx, _) = schema.get_column(case_col_name)?;
        let case_key_info = crate::executor::arena_join::precompute_key_offset(schema, case_col_idx);

        let prefix_col_indices: Vec<usize> = prefix_cols.iter().filter_map(|name| {
            schema.get_column(name).map(|(idx, _)| idx)
        }).collect();
        let prefix_fast = crate::executor::arena_join::build_fast_encoders(schema, &prefix_col_indices);

        let mut out = Vec::with_capacity(64 * 1024);

        // ROW_HEADER
        let mut hdr = Vec::with_capacity(64);
        let total_cols = prefix_cols.len() + 1; // prefix columns + CASE result
        hdr.extend_from_slice(&(total_cols as u16).to_le_bytes());
        for name in &prefix_cols {
            let nb = name.as_bytes();
            hdr.extend_from_slice(&(nb.len() as u16).to_le_bytes());
            hdr.extend_from_slice(nb);
            hdr.push(0x01); // INT32 type for id
        }
        // CASE result column
        let case_name = b"case_result";
        hdr.extend_from_slice(&(case_name.len() as u16).to_le_bytes());
        hdr.extend_from_slice(case_name);
        hdr.push(0x05); // STRING type
        Self::append_msg(&mut out, MSG_ROW_HEADER, &hdr);

        // Scan and evaluate
        let mut row_count: u64 = 0;
        let mvcc = info.mvcc_enabled;
        let mut current_pid = info.first_page_id;

        while current_pid.0 != crate::common::INVALID_PAGE_ID {
            let guard = cbpm.read_page_direct(current_pid).ok()?;
            let page = guard.data();
            let num_slots = crate::storage::heap_page::get_num_slots(page);

            for slot in 0..num_slots {
                if let Some((off, len)) = crate::storage::heap_page::get_tuple_slice(page, slot) {
                    let raw = &page[off..off + len];
                    let tuple_data = if mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                        let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                        if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                        &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                    } else { raw };

                    let row_start = out.len();
                    out.extend_from_slice(&[MSG_ROW, 0, 0, 0, 0]);

                    // Encode prefix columns
                    if let Some(ref pf) = prefix_fast {
                        for (i, _) in prefix_cols.iter().enumerate() {
                            pf[i].encode(&mut out, tuple_data);
                        }
                    } else {
                        let col_info = crate::executor::arena_join::compute_column_info(schema);
                        for &ci in &prefix_col_indices {
                            crate::executor::arena_join::encode_single_column_raw(&mut out, tuple_data, schema, &col_info, ci);
                        }
                    }

                    // Evaluate CASE on raw integer
                    let case_val = if let Some((bml, off, is32)) = case_key_info {
                        crate::executor::arena_join::read_key_fast(tuple_data, bml, off, is32, case_col_idx)
                    } else {
                        crate::tuple::tuple::read_column_i64_raw(tuple_data, schema, case_col_idx)
                    };

                    let result_str = if let Some(v) = case_val {
                        let mut result = else_val;
                        for &(threshold, val) in &branches {
                            if v > threshold { result = val; break; }
                        }
                        result
                    } else { else_val };

                    // Encode CASE result as string
                    let rb = result_str.as_bytes();
                    out.push(VAL_STRING);
                    out.extend_from_slice(&(rb.len() as u32).to_le_bytes());
                    out.extend_from_slice(rb);

                    let payload_len = (out.len() - row_start - 5) as u32;
                    out[row_start + 1..row_start + 5].copy_from_slice(&payload_len.to_le_bytes());
                    row_count += 1;
                }
            }

            current_pid = crate::common::PageId(crate::storage::heap_page::get_next_page_id(page));
        }

        Self::append_msg(&mut out, MSG_DONE, &row_count.to_le_bytes());
        Some(out)
    }

    /// Fast JOIN detection: parse table names, join type, ON columns, and projection
    /// directly from the SQL string — zero parser/planner invocation.
    ///
    /// Handles: SELECT [cols] FROM t1 [alias] {INNER|LEFT} JOIN t2 [alias] ON t1.c = t2.c
    fn try_fast_join_query(&self, sql: &str) -> Option<Vec<u8>> {
        let upper = sql.trim().to_uppercase();
        if !upper.contains("JOIN") { return None; }
        if upper.contains("RIGHT") || upper.contains("FULL") || upper.contains("CROSS")
            || upper.contains("WHERE") || upper.contains("GROUP") || upper.contains("ORDER")
            || upper.contains("LIMIT") || upper.contains("UNION") || upper.contains("HAVING") {
            return None; // only simple JOINs — complex queries go through full planner
        }

        // Determine join type
        let is_left = upper.contains("LEFT");

        // Extract projection columns: between SELECT and FROM
        let select_end = upper.find("FROM")?;
        let select_part = sql.trim()[6..select_end].trim(); // skip "SELECT"
        let projection_cols: Option<Vec<String>> = if select_part == "*" {
            None
        } else {
            let cols: Vec<String> = select_part.split(',').map(|c| {
                let c = c.trim();
                // Handle table.column — extract just the column name
                if let Some(dot_pos) = c.rfind('.') {
                    c[dot_pos + 1..].trim().to_string()
                } else {
                    c.to_string()
                }
            }).collect();
            if cols.is_empty() { return None; }
            Some(cols)
        };

        // Extract table names and aliases from: FROM t1 [a1] {INNER|LEFT} JOIN t2 [a2] ON ...
        let from_start = select_end + 4; // skip "FROM"
        let join_keyword_pos = if is_left {
            upper.find("LEFT JOIN")?
        } else {
            // Could be "INNER JOIN" or just "JOIN"
            upper.find("INNER JOIN").or_else(|| upper.find(" JOIN ").map(|p| p + 1).map(|p| p - 1))?
        };

        // Left table: between FROM and JOIN keyword
        let left_part = sql.trim()[from_start..join_keyword_pos].trim();
        let left_tokens: Vec<&str> = left_part.split_whitespace().collect();
        if left_tokens.is_empty() { return None; }
        let left_table = left_tokens[0].trim_matches(|c: char| c == '`' || c == '"' || c == '[' || c == ']');
        let left_alias = if left_tokens.len() > 1 && !left_tokens[1].eq_ignore_ascii_case("INNER") && !left_tokens[1].eq_ignore_ascii_case("LEFT") {
            Some(left_tokens[1].trim_matches(|c: char| c == '`' || c == '"'))
        } else { None };

        // Right table: after JOIN keyword, before ON
        let join_end = if is_left { join_keyword_pos + 9 } else {
            if let Some(p) = upper.find("INNER JOIN") { p + 10 }
            else { upper.find(" JOIN ")? + 6 }
        };
        let on_pos = upper.find(" ON ")?;
        let right_part = sql.trim()[join_end..on_pos].trim();
        let right_tokens: Vec<&str> = right_part.split_whitespace().collect();
        if right_tokens.is_empty() { return None; }
        let right_table = right_tokens[0].trim_matches(|c: char| c == '`' || c == '"' || c == '[' || c == ']');
        let right_alias = if right_tokens.len() > 1 {
            Some(right_tokens[1].trim_matches(|c: char| c == '`' || c == '"'))
        } else { None };

        // Extract ON condition: t1.col = t2.col
        let on_part = sql.trim()[on_pos + 4..].trim().trim_end_matches(';').trim();
        let eq_pos = on_part.find('=')?;
        let on_left = on_part[..eq_pos].trim();
        let on_right = on_part[eq_pos + 1..].trim();

        // Extract column names from qualified references
        let left_on_col = if let Some(dot) = on_left.rfind('.') { &on_left[dot + 1..] } else { on_left };
        let right_on_col = if let Some(dot) = on_right.rfind('.') { &on_right[dot + 1..] } else { on_right };

        // Build the ON expression as AST node
        let on_expr = crate::sql::ast::Expr::BinaryOp {
            left: Box::new(crate::sql::ast::Expr::ColumnRef {
                table: None,
                column: left_on_col.to_string(),
            }),
            op: crate::sql::ast::BinaryOperator::Eq,
            right: Box::new(crate::sql::ast::Expr::ColumnRef {
                table: None,
                column: right_on_col.to_string(),
            }),
        };

        let catalog_guard = self.db.catalog_ref();
        let cbpm = self.db.cbpm_ref();

        if is_left {
            crate::executor::arena_join::try_arena_left_join_projected(
                left_table, left_alias, right_table, right_alias,
                &on_expr, &*catalog_guard, cbpm,
                projection_cols.as_deref(),
            )
        } else {
            // Try arena INNER JOIN
            if let Some(wire_bytes) = crate::executor::arena_join::try_arena_join_projected(
                left_table, left_alias, right_table, right_alias,
                &on_expr, &*catalog_guard, cbpm,
                projection_cols.as_deref(),
            ) {
                return Some(wire_bytes);
            }

            // Fallback to columnar join
            let cr = crate::executor::columnar_join::try_columnar_inner_join(
                left_table, left_alias, right_table, right_alias,
                &on_expr, &*catalog_guard, cbpm, None,
            )?;
            Some(crate::executor::columnar_join::columnar_to_forgewire(&cr))
        }
    }

    // -- Prepared statements --

    fn handle_prepare(&mut self, payload: &[u8]) -> io::Result<()> {
        let sql = std::str::from_utf8(payload).unwrap_or("").to_string();
        let handle = self.next_handle;
        self.next_handle += 1;
        self.prepared.insert(handle, sql);

        let mut buf = Vec::with_capacity(4);
        buf.extend_from_slice(&handle.to_le_bytes());
        self.write_msg(MSG_PREPARE_OK, &buf)?;
        self.flush()
    }

    fn handle_execute(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() < 4 {
            return self.send_error("execute: missing handle");
        }
        let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let sql = match self.prepared.get(&handle) {
            Some(s) => s.clone(),
            None => return self.send_error("execute: invalid handle"),
        };

        // Read parameter values from payload[4..]
        let params = Self::decode_params(&payload[4..]);

        // Substitute @p0, @p1, ... with actual values
        let mut final_sql = sql;
        for (i, val) in params.iter().enumerate() {
            let placeholder = format!("@p{}", i);
            let literal = match val {
                Value::Integer(n) => format!("{}", n),
                Value::BigInt(n) => format!("{}", n),
                Value::Float(f) => format!("{}", f),
                Value::Boolean(b) => if *b { "1".into() } else { "0".into() },
                Value::Varchar(s) => format!("'{}'", s.replace('\'', "''")),
                Value::Null => "NULL".into(),
                _ => format!("'{}'", val),
            };
            final_sql = final_sql.replace(&placeholder, &literal);
        }

        let result = self.db.execute_sql_session(&final_sql, &mut self.current_txn_id);
        match result {
            Ok(r) => self.send_result(&r)?,
            Err(e) => self.send_error(&format!("{}", e))?,
        }
        self.flush()
    }

    fn handle_close(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() >= 4 {
            let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            self.prepared.remove(&handle);
        }
        self.write_msg(MSG_DONE, &0u64.to_le_bytes())?;
        self.flush()
    }

    // -- Batch INSERT --
    //
    // One TCP round-trip for N rows. The server parses zero SQL,
    // acquires the table lock once, and inserts all rows in a tight loop.
    //
    // Payload format:
    //   table_name_len: u16 LE
    //   table_name: UTF-8 bytes
    //   col_count: u16 LE
    //   row_count: u32 LE
    //   For each row × col: tag-encoded value

    fn handle_batch_insert(&mut self, payload: &[u8]) -> io::Result<()> {
        let mut pos = 0;
        if payload.len() < 8 { return self.send_error("batch_insert: payload too short"); }

        // Table name
        let name_len = u16::from_le_bytes([payload[pos], payload[pos+1]]) as usize; pos += 2;
        if pos + name_len > payload.len() { return self.send_error("batch_insert: bad name_len"); }
        let table_name = std::str::from_utf8(&payload[pos..pos+name_len]).unwrap_or(""); pos += name_len;

        // Column count, row count
        if pos + 6 > payload.len() { return self.send_error("batch_insert: missing counts"); }
        let col_count = u16::from_le_bytes([payload[pos], payload[pos+1]]) as usize; pos += 2;
        let row_count = u32::from_le_bytes([payload[pos], payload[pos+1], payload[pos+2], payload[pos+3]]) as usize; pos += 4;

        // Build INSERT statement with all rows as VALUES tuples.
        // This avoids per-row SQL parsing — one parse for all rows.
        let mut values_parts = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let mut cols = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                if pos >= payload.len() { break; }
                let tag = payload[pos]; pos += 1;
                match tag {
                    VAL_NULL => cols.push("NULL".to_string()),
                    VAL_INT32 => {
                        if pos + 4 > payload.len() { break; }
                        let v = i32::from_le_bytes([payload[pos], payload[pos+1], payload[pos+2], payload[pos+3]]);
                        cols.push(format!("{}", v)); pos += 4;
                    }
                    VAL_INT64 => {
                        if pos + 8 > payload.len() { break; }
                        let v = i64::from_le_bytes(payload[pos..pos+8].try_into().unwrap());
                        cols.push(format!("{}", v)); pos += 8;
                    }
                    VAL_FLOAT64 => {
                        if pos + 8 > payload.len() { break; }
                        let v = f64::from_le_bytes(payload[pos..pos+8].try_into().unwrap());
                        cols.push(format!("{}", v)); pos += 8;
                    }
                    VAL_BOOL => {
                        if pos >= payload.len() { break; }
                        cols.push(if payload[pos] != 0 { "1" } else { "0" }.to_string()); pos += 1;
                    }
                    VAL_STRING => {
                        if pos + 4 > payload.len() { break; }
                        let slen = u32::from_le_bytes([payload[pos], payload[pos+1], payload[pos+2], payload[pos+3]]) as usize; pos += 4;
                        if pos + slen > payload.len() { break; }
                        let s = std::str::from_utf8(&payload[pos..pos+slen]).unwrap_or("");
                        cols.push(format!("'{}'", s.replace('\'', "''"))); pos += slen;
                    }
                    _ => cols.push("NULL".to_string()),
                }
            }
            values_parts.push(format!("({})", cols.join(",")));
        }

        if values_parts.is_empty() {
            self.write_msg(MSG_DONE, &0u64.to_le_bytes())?;
            return self.flush();
        }

        // Single INSERT with all rows — one parse, one lock, one transaction
        let sql = format!("INSERT INTO {} VALUES {}", table_name, values_parts.join(","));
        let result = self.db.execute_sql_session(&sql, &mut self.current_txn_id);
        match result {
            Ok(r) => {
                self.write_msg(MSG_DONE, &(r.rows_affected as u64).to_le_bytes())?;
            }
            Err(e) => {
                self.send_error(&format!("{}", e))?;
                return Ok(());
            }
        }
        self.flush()
    }

    // -- Param decoding --

    fn decode_params(mut data: &[u8]) -> Vec<Value> {
        let mut params = Vec::new();
        while !data.is_empty() {
            let tag = data[0];
            data = &data[1..];
            match tag {
                VAL_NULL => params.push(Value::Null),
                VAL_INT32 => {
                    if data.len() < 4 { break; }
                    let v = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                    params.push(Value::Integer(v));
                    data = &data[4..];
                }
                VAL_INT64 => {
                    if data.len() < 8 { break; }
                    let v = i64::from_le_bytes(data[..8].try_into().unwrap());
                    params.push(Value::BigInt(v));
                    data = &data[8..];
                }
                VAL_FLOAT64 => {
                    if data.len() < 8 { break; }
                    let v = f64::from_le_bytes(data[..8].try_into().unwrap());
                    params.push(Value::Float(v));
                    data = &data[8..];
                }
                VAL_BOOL => {
                    if data.is_empty() { break; }
                    params.push(Value::Boolean(data[0] != 0));
                    data = &data[1..];
                }
                VAL_STRING => {
                    if data.len() < 4 { break; }
                    let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                    data = &data[4..];
                    if data.len() < len { break; }
                    let s = String::from_utf8_lossy(&data[..len]).to_string();
                    params.push(Value::Varchar(s));
                    data = &data[len..];
                }
                _ => break,
            }
        }
        params
    }

    // -- Result encoding --

    fn send_result(&mut self, result: &crate::executor::executor::ExecuteResult) -> io::Result<()> {
        if result.columns.is_empty() {
            self.write_msg(MSG_DONE, &(result.rows_affected as u64).to_le_bytes())?;
            return Ok(());
        }

        // Batch everything into one buffer: ROW_HEADER + all ROWs + DONE
        // This minimizes syscalls — one big write instead of N+2 small ones.
        let estimated_size = 128 + result.rows.len() * result.columns.len() * 10;
        let mut out = Vec::with_capacity(estimated_size);

        // ROW_HEADER message
        let mut hdr = Vec::with_capacity(64);
        hdr.extend_from_slice(&(result.columns.len() as u16).to_le_bytes());
        for (i, col) in result.columns.iter().enumerate() {
            let name_bytes = col.as_bytes();
            hdr.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            hdr.extend_from_slice(name_bytes);
            let type_id = if let Some(first_row) = result.rows.first() {
                if i < first_row.len() { Self::value_type_id(&first_row[i]) } else { VAL_STRING }
            } else { VAL_STRING };
            hdr.push(type_id);
        }
        Self::append_msg(&mut out, MSG_ROW_HEADER, &hdr);

        // ROW messages — encode directly into the output buffer
        let num_cols = result.columns.len();
        for row in &result.rows {
            let row_start = out.len();
            // Reserve space for msg header (5 bytes) — will fill in length after
            out.extend_from_slice(&[MSG_ROW, 0, 0, 0, 0]);
            for val in row.iter().take(num_cols) {
                Self::encode_value(&mut out, val);
            }
            // Patch the length field
            let payload_len = (out.len() - row_start - 5) as u32;
            out[row_start + 1..row_start + 5].copy_from_slice(&payload_len.to_le_bytes());
        }

        // DONE message
        Self::append_msg(&mut out, MSG_DONE, &(result.rows.len() as u64).to_le_bytes());

        // Single write for entire result set
        self.writer.write_all(&out)?;
        Ok(())
    }

    /// Append a framed message to a buffer (no I/O).
    #[inline]
    fn append_msg(buf: &mut Vec<u8>, msg_type: u8, payload: &[u8]) {
        buf.push(msg_type);
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload);
    }

    fn send_error(&mut self, msg: &str) -> io::Result<()> {
        let bytes = msg.as_bytes();
        let mut buf = Vec::with_capacity(2 + bytes.len());
        buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(bytes);
        self.write_msg(MSG_ERROR, &buf)?;
        self.flush()
    }

    #[inline]
    fn value_type_id(val: &Value) -> u8 {
        match val {
            Value::Null => VAL_NULL,
            Value::Integer(_) => VAL_INT32,
            Value::BigInt(_) => VAL_INT64,
            Value::Float(_) => VAL_FLOAT64,
            Value::Boolean(_) => VAL_BOOL,
            _ => VAL_STRING,
        }
    }

    #[inline]
    fn encode_value(buf: &mut Vec<u8>, val: &Value) {
        match val {
            Value::Null => buf.push(VAL_NULL),
            Value::Integer(n) => { buf.push(VAL_INT32); buf.extend_from_slice(&n.to_le_bytes()); }
            Value::BigInt(n) => { buf.push(VAL_INT64); buf.extend_from_slice(&n.to_le_bytes()); }
            Value::Float(f) => { buf.push(VAL_FLOAT64); buf.extend_from_slice(&f.to_le_bytes()); }
            Value::Boolean(b) => { buf.push(VAL_BOOL); buf.push(if *b { 1 } else { 0 }); }
            _ => {
                let s = val.to_string();
                let bytes = s.as_bytes();
                buf.push(VAL_STRING);
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }
}

impl Drop for ForgeWireConn {
    fn drop(&mut self) {
        if let Some(txn_id) = self.current_txn_id.take() {
            self.db.abort_transaction(txn_id);
        }
    }
}
