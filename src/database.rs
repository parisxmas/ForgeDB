use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use crate::catalog::Catalog;
use crate::error::{ForgeError, Result};
use crate::executor::executor::{DmlContext, ExecuteResult, ExecutorContext, ReadContext};
use crate::executor::{self};
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::planner::plan::PlanNode;
use crate::planner::statistics::{ColumnStatistics, TableStatistics};
use crate::planner::Planner;
use crate::sql;
use crate::sql::ast::{AlterTableOp, Expr, LiteralValue, Statement, TriggerDef, TriggerEvent};
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::DiskManager;
use crate::tuple::schema::Column;
use crate::tuple::types::Value;
use crate::common::{TxnId, PageId, PAGE_SIZE};
use crate::txn::{TransactionManager, TxnContext, UndoLog, LockManager, IsolationLevel};

/// Metadata for a partitioned table.
#[derive(Debug, Clone)]
struct PartitionMeta {
    /// The column used for partitioning.
    column: String,
    /// Ordered list of (partition_name, upper_bound_value).
    /// For RANGE partitions, rows with value < upper_bound go to this partition.
    /// A value of i64::MAX means MAXVALUE.
    range_partitions: Vec<(String, i64)>,
    /// The column definitions for creating sub-tables.
    columns_sql: String,
}

/// Top-level database handle with page-level concurrent access.
///
/// Uses ConcurrentBufferPool with per-page RwLocks so multiple threads
/// can read/write different pages simultaneously. Only same-page access
/// serializes. SQL parsing and planning run fully parallel.
pub struct Database {
    cbpm: ConcurrentBufferPool,
    catalog: RwLock<Catalog>,
    txn_manager: Mutex<TransactionManager>,
    indexes: RwLock<Vec<(String, BTreeIndex)>>,
    clustered_indexes: RwLock<HashMap<String, ClusteredIndex>>,
    auto_increment_counters: Mutex<HashMap<String, i64>>,
    /// Savepoints: maps txn_id -> list of (savepoint_name, undo_log_position)
    savepoints: Mutex<HashMap<TxnId, Vec<(String, usize)>>>,
    /// Sequences: maps sequence_name -> (current_value, increment)
    sequences: Mutex<HashMap<String, (i64, i64)>>,
    /// Row-level lock manager with deadlock detection
    lock_manager: LockManager,
    #[allow(dead_code)]
    db_path: PathBuf,
    /// Stored procedures: name -> (params, body SQL statements)
    procedures: Mutex<HashMap<String, (Vec<(String, crate::tuple::types::DataType)>, Vec<String>)>>,
    /// Triggers: table_name (lowercase) -> list of trigger definitions
    triggers: Mutex<HashMap<String, Vec<TriggerDef>>>,
    /// Users: username (lowercase) -> (password, set of privileges)
    users: Mutex<HashMap<String, (String, HashSet<String>)>>,
    /// Prepared statements: name -> SQL text
    prepared_stmts: Mutex<HashMap<String, String>>,
    /// Query plan cache: SQL text -> PlanNode (for SELECT queries)
    plan_cache: RwLock<HashMap<String, PlanNode>>,
    /// Cursor state: cursor_name -> (rows, column_names, current_position)
    cursors: Mutex<HashMap<String, (Vec<Vec<Value>>, Vec<String>, usize)>>,
    /// Max memory per query in bytes (0 = unlimited)
    max_memory_per_query: Mutex<usize>,
    /// Partitioned tables: base_table_name (lowercase) -> PartitionMeta
    partitions: RwLock<HashMap<String, PartitionMeta>>,
    /// Fast check: true if any partitions exist (avoids RwLock for common case)
    has_partitions: std::sync::atomic::AtomicBool,
    /// Pending WAL entries for session transactions (batched until COMMIT).
    /// Maps txn_id -> accumulated WAL page entries to write on commit.
    pending_wal: Mutex<HashMap<TxnId, Vec<(TxnId, PageId, Box<[u8; PAGE_SIZE]>, Box<[u8; PAGE_SIZE]>)>>>,
}

impl Database {
    /// Create a new database at the given directory path.
    pub fn new(path: &str) -> Result<Self> {
        let db_path = PathBuf::from(path);
        std::fs::create_dir_all(&db_path)?;

        let data_file = db_path.join("forgedb.data");
        let catalog_file = db_path.join("forgedb.catalog");
        let wal_file = db_path.join("forgedb.wal");

        let disk_manager = DiskManager::new(data_file.to_str().ok_or_else(|| ForgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid data file path")))?)?;
        let cbpm = ConcurrentBufferPool::new(1024, disk_manager);
        let catalog = Catalog::new(catalog_file.to_str().ok_or_else(|| ForgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid catalog file path")))?);
        let txn_manager = TransactionManager::new(wal_file.to_str().ok_or_else(|| ForgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid WAL file path")))?)?;

        Ok(Self {
            cbpm,
            catalog: RwLock::new(catalog),
            txn_manager: Mutex::new(txn_manager),
            indexes: RwLock::new(Vec::new()),
            clustered_indexes: RwLock::new(HashMap::new()),
            auto_increment_counters: Mutex::new(HashMap::new()),
            savepoints: Mutex::new(HashMap::new()),
            sequences: Mutex::new(HashMap::new()),
            lock_manager: LockManager::new(std::time::Duration::from_secs(30)),
            db_path,
            procedures: Mutex::new(HashMap::new()),
            triggers: Mutex::new(HashMap::new()),
            users: Mutex::new(HashMap::new()),
            prepared_stmts: Mutex::new(HashMap::new()),
            plan_cache: RwLock::new(HashMap::new()),
            cursors: Mutex::new(HashMap::new()),
            max_memory_per_query: Mutex::new(0),
            partitions: RwLock::new(HashMap::new()),
            has_partitions: std::sync::atomic::AtomicBool::new(false),
            pending_wal: Mutex::new(HashMap::new()),
        })
    }

    /// Open an existing database at the given directory path.
    pub fn open(path: &str) -> Result<Self> {
        let db_path = PathBuf::from(path);

        let data_file = db_path.join("forgedb.data");
        let catalog_file = db_path.join("forgedb.catalog");
        let wal_file = db_path.join("forgedb.wal");

        let disk_manager = DiskManager::new(data_file.to_str().ok_or_else(|| ForgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid data file path")))?)?;
        let cbpm = ConcurrentBufferPool::new(1024, disk_manager);
        let catalog = Catalog::load(catalog_file.to_str().ok_or_else(|| ForgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid catalog file path")))?)?;
        let mut txn_manager = TransactionManager::new(wal_file.to_str().ok_or_else(|| ForgeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid WAL file path")))?)?;

        // Recovery via a temporary LocalBpm — drop it before moving cbpm
        {
            let mut local = LocalBpm::new(&cbpm);
            txn_manager.recover(&mut local)?;
        }

        Ok(Self {
            cbpm,
            catalog: RwLock::new(catalog),
            txn_manager: Mutex::new(txn_manager),
            indexes: RwLock::new(Vec::new()),
            clustered_indexes: RwLock::new(HashMap::new()),
            auto_increment_counters: Mutex::new(HashMap::new()),
            savepoints: Mutex::new(HashMap::new()),
            sequences: Mutex::new(HashMap::new()),
            lock_manager: LockManager::new(std::time::Duration::from_secs(30)),
            db_path,
            procedures: Mutex::new(HashMap::new()),
            triggers: Mutex::new(HashMap::new()),
            users: Mutex::new(HashMap::new()),
            prepared_stmts: Mutex::new(HashMap::new()),
            plan_cache: RwLock::new(HashMap::new()),
            cursors: Mutex::new(HashMap::new()),
            max_memory_per_query: Mutex::new(0),
            partitions: RwLock::new(HashMap::new()),
            has_partitions: std::sync::atomic::AtomicBool::new(false),
            pending_wal: Mutex::new(HashMap::new()),
        })
    }

    /// Set the transaction lock timeout.
    pub fn set_lock_timeout(&self, timeout: std::time::Duration) {
        self.lock_manager.set_timeout(timeout);
    }

    /// Parse, plan, and execute a SQL statement (auto-transaction mode).
    pub fn execute_sql(&self, sql_text: &str) -> Result<ExecuteResult> {
        let mut session_txn = None;
        self.execute_sql_session(sql_text, &mut session_txn)
    }

    /// Parse, plan, and execute a SQL statement with session transaction state.
    /// `session_txn` tracks the active multi-statement transaction (BEGIN/COMMIT/ROLLBACK).
    pub fn execute_sql_session(
        &self,
        sql_text: &str,
        session_txn: &mut Option<TxnId>,
    ) -> Result<ExecuteResult> {
        // Fast path: SELECT/INSERT/UPDATE/DELETE/BEGIN/COMMIT/ROLLBACK
        // skip the expensive to_uppercase + 30 starts_with checks
        let trimmed = sql_text.trim();
        let first2 = if trimmed.len() >= 2 {
            let b0 = trimmed.as_bytes()[0].to_ascii_uppercase();
            let b1 = trimmed.as_bytes()[1].to_ascii_uppercase();
            (b0, b1)
        } else { (0, 0) };
        // Only fast-path pure SQL DML/DQL — skip anything that needs pre-parse handling.
        // Check 6 chars to distinguish DELETE from DECLARE, COMMIT from CLOSE, etc.
        let is_fast_stmt = trimmed.len() >= 6 && {
            let pfx = &trimmed.as_bytes()[..6];
            pfx.eq_ignore_ascii_case(b"SELECT")
                || pfx.eq_ignore_ascii_case(b"INSERT")
                || pfx.eq_ignore_ascii_case(b"UPDATE")
                || pfx.eq_ignore_ascii_case(b"DELETE")
                || pfx.eq_ignore_ascii_case(b"ANALYZ")
        };
        if is_fast_stmt {
            // Skip pre-parse string matching — go directly to SQL parse + plan
            let sql_text_owned;
            let sql_ref = if self.has_partitions.load(std::sync::atomic::Ordering::Relaxed) {
                let upper = trimmed.to_uppercase();
                sql_text_owned = self.resolve_partitioned_sql(trimmed, &upper)?;
                sql_text_owned.as_str()
            } else {
                trimmed
            };
            let stmt = crate::sql::parse(sql_ref)?;
            return self.execute_parsed_session(stmt, session_txn, sql_ref);
        }

        // Handle SET LOCK_TIMEOUT before parsing
        let upper = trimmed.to_uppercase();
        if upper.starts_with("SET LOCK_TIMEOUT ") {
            if let Some(ms_str) = upper.strip_prefix("SET LOCK_TIMEOUT ") {
                if let Ok(ms) = ms_str.trim().trim_end_matches(';').parse::<u64>() {
                    self.lock_manager.set_timeout(std::time::Duration::from_millis(ms));
                }
            }
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }
        // Handle SET TRANSACTION ISOLATION LEVEL
        if upper.starts_with("SET TRANSACTION ISOLATION LEVEL") {
            // Accept silently — ForgeDB uses snapshot isolation (RepeatableRead equivalent)
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }

        // --- Pre-parse string matching for statements sqlparser doesn't handle ---

        // CREATE PROCEDURE name AS BEGIN ... END
        if upper.starts_with("CREATE PROC") {
            return self.handle_create_procedure(sql_text);
        }
        // EXEC / EXECUTE procedure_name [args]
        if upper.starts_with("EXEC ") || upper.starts_with("EXECUTE ") {
            // Distinguish EXEC proc from EXECUTE prepared_stmt
            // EXEC is always stored procedure; EXECUTE could be either
            let rest = if upper.starts_with("EXEC ") {
                sql_text.trim()[5..].trim()
            } else {
                sql_text.trim()[8..].trim()
            };
            // Check if it's a prepared statement execution
            let name = rest.split_whitespace().next().unwrap_or("").to_string();
            let is_prepared = {
                let stmts = self.prepared_stmts.lock().unwrap();
                stmts.contains_key(&name.to_lowercase())
            };
            if is_prepared {
                return self.handle_execute_prepared(&name, session_txn);
            }
            return self.handle_exec_procedure(rest, session_txn);
        }
        // CREATE TRIGGER
        if upper.starts_with("CREATE TRIGGER") {
            return self.handle_create_trigger(sql_text);
        }
        // DROP TRIGGER
        if upper.starts_with("DROP TRIGGER") {
            let name = upper.trim_start_matches("DROP TRIGGER")
                .trim().trim_end_matches(';').trim().to_string();
            let mut triggers = self.triggers.lock().unwrap();
            for (_table, trigs) in triggers.iter_mut() {
                trigs.retain(|t| !t.name.eq_ignore_ascii_case(&name));
            }
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }
        // CREATE USER
        if upper.starts_with("CREATE USER") || upper.starts_with("CREATE LOGIN") {
            return self.handle_create_user(sql_text);
        }
        // DROP USER
        if upper.starts_with("DROP USER") || upper.starts_with("DROP LOGIN") {
            let name = sql_text.trim()
                .split_whitespace().nth(2).unwrap_or("").trim_end_matches(';').to_string();
            let mut users = self.users.lock().unwrap();
            users.remove(&name.to_lowercase());
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }
        // DROP PROCEDURE
        if upper.starts_with("DROP PROCEDURE ") || upper.starts_with("DROP PROC ") {
            let name = sql_text.trim()
                .split_whitespace().nth(2).unwrap_or("").trim_end_matches(';').to_string();
            self.procedures.lock().unwrap().remove(&name.to_lowercase());
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }
        // DROP SEQUENCE
        if upper.starts_with("DROP SEQUENCE ") {
            let name = sql_text.trim()
                .split_whitespace().nth(2).unwrap_or("").trim_end_matches(';').to_string();
            self.sequences.lock().unwrap().remove(&name.to_lowercase());
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }
        // GRANT
        if upper.starts_with("GRANT ") {
            return self.handle_grant(sql_text);
        }
        // REVOKE
        if upper.starts_with("REVOKE ") {
            return self.handle_revoke(sql_text);
        }
        // PREPARE name AS 'sql'
        if upper.starts_with("PREPARE ") {
            return self.handle_prepare(sql_text);
        }
        // BACKUP DATABASE
        if upper.starts_with("BACKUP DATABASE") || upper.starts_with("BACKUP DB") {
            return self.handle_backup(sql_text);
        }
        // RESTORE DATABASE
        if upper.starts_with("RESTORE DATABASE") || upper.starts_with("RESTORE DB") {
            return self.handle_restore(sql_text);
        }
        // DECLARE ... CURSOR FOR ...
        if upper.starts_with("DECLARE ") && upper.contains("CURSOR") {
            return self.handle_declare_cursor(sql_text);
        }
        // OPEN cursor
        if upper.starts_with("OPEN ") {
            let name = sql_text.trim()[5..].trim().trim_end_matches(';').trim().to_string();
            return self.handle_open_cursor(&name, session_txn);
        }
        // FETCH NEXT FROM cursor
        if upper.starts_with("FETCH ") {
            return self.handle_fetch_cursor(sql_text);
        }
        // CLOSE cursor
        if upper.starts_with("CLOSE ") {
            let name = sql_text.trim()[6..].trim().trim_end_matches(';').trim().to_string();
            let mut cursors = self.cursors.lock().unwrap();
            // Keep cursor data but could mark as closed
            if cursors.contains_key(&name.to_lowercase()) {
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                });
            }
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }
        // DEALLOCATE cursor
        if upper.starts_with("DEALLOCATE ") {
            let name = sql_text.trim()[11..].trim().trim_end_matches(';')
                .trim().trim_start_matches("CURSOR").trim().to_string();
            let mut cursors = self.cursors.lock().unwrap();
            cursors.remove(&name.to_lowercase());
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }
        // SET MAX_MEMORY
        if upper.starts_with("SET MAX_MEMORY ") || upper.starts_with("SET MAX_MEMORY_PER_QUERY ") {
            let val_str = upper.rsplit_once(' ').map(|(_, v)| v).unwrap_or("0");
            let val_str = val_str.trim_end_matches(';');
            if let Ok(bytes) = val_str.parse::<usize>() {
                let mut mem = self.max_memory_per_query.lock().unwrap();
                *mem = bytes;
            }
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }

        // CREATE TABLE ... PARTITION BY RANGE — handle before parsing
        if upper.starts_with("CREATE TABLE") && upper.contains("PARTITION BY RANGE") {
            return self.handle_create_partitioned_table(sql_text);
        }
        // CREATE FULLTEXT INDEX — handle before parsing
        if upper.starts_with("CREATE FULLTEXT INDEX") {
            // Accept silently — full-text search uses runtime scan
            return Ok(ExecuteResult {
                rows: vec![], columns: vec![],
                rows_affected: 0, last_insert_id: 0, message: "OK".into(),
            });
        }

        // Check if this is an INSERT/SELECT targeting a partitioned table and redirect
        // Fast path: skip partition resolution when no partitions exist (lock-free check)
        let sql_text_resolved;
        let sql_text = if self.has_partitions.load(std::sync::atomic::Ordering::Relaxed) {
            sql_text_resolved = self.resolve_partitioned_sql(sql_text, &upper)?;
            &sql_text_resolved
        } else {
            sql_text
        };

        // Phase 1: Parse
        let stmt = sql::parse(sql_text)?;
        self.execute_parsed_session(stmt, session_txn, sql_text)
    }

    /// Execute a pre-parsed SQL statement with session transaction state.
    fn execute_parsed_session(
        &self,
        stmt: Statement,
        session_txn: &mut Option<TxnId>,
        sql_text: &str,
    ) -> Result<ExecuteResult> {
        // Phase 2: Handle transaction control and metadata statements
        match &stmt {
            Statement::StartTransaction => {
                // If already in a transaction, implicitly commit it (MySQL behavior)
                if let Some(old_txn) = session_txn.take() {
                    let mut tm = self.txn_manager.lock().unwrap();
                    if tm.is_active(old_txn) {
                        let _ = tm.commit(old_txn);
                        drop(tm);
                        let _ = self.cbpm.flush_all();
                    }
                }
                let mut tm = self.txn_manager.lock().unwrap();
                let txn_id = tm.begin()?;
                *session_txn = Some(txn_id);
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                });
            }
            Statement::Commit => {
                if let Some(txn_id) = session_txn.take() {
                    // Flush batched WAL entries before commit
                    let batched_wal = {
                        let mut pending = self.pending_wal.lock().unwrap();
                        pending.remove(&txn_id).unwrap_or_default()
                    };
                    let mut tm = self.txn_manager.lock().unwrap();
                    if tm.is_active(txn_id) {
                        // Write all batched WAL entries in one burst
                        for (tid, page_id, before, after) in &batched_wal {
                            let _ = tm.log_page_write(*tid, *page_id, before, after);
                        }
                        tm.commit(txn_id)?;
                        drop(tm);
                        self.lock_manager.release_all(txn_id);
                        // Clean up savepoints for this transaction
                        self.savepoints.lock().unwrap().remove(&txn_id);
                        self.cbpm.flush_all()?;
                    }
                }
                // COMMIT without BEGIN is a no-op
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                });
            }
            Statement::Rollback => {
                if let Some(txn_id) = session_txn.take() {
                    // Discard batched WAL entries on rollback
                    self.pending_wal.lock().unwrap().remove(&txn_id);

                    let mut tm = self.txn_manager.lock().unwrap();
                    if tm.is_active(txn_id) {
                        // Apply undo log before aborting
                        let undo_log = tm.take_undo_log(txn_id);
                        drop(tm);

                        if let Some(ref log) = undo_log {
                            if !log.is_empty() {
                                let mut local_bpm = LocalBpm::new(&self.cbpm);
                                let catalog = self.catalog.read().unwrap();
                                crate::txn::undo::apply_undo(log, &mut local_bpm, &catalog)?;
                                drop(local_bpm);
                                drop(catalog);
                            }
                        }

                        let mut tm = self.txn_manager.lock().unwrap();
                        tm.abort(txn_id)?;
                        drop(tm);
                        self.lock_manager.release_all(txn_id);
                        // Clean up savepoints for this transaction
                        self.savepoints.lock().unwrap().remove(&txn_id);
                        // Flush to make undo changes visible
                        self.cbpm.flush_all()?;
                    }
                }
                // ROLLBACK without BEGIN is a no-op
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                });
            }
            Statement::Savepoint { ref name } => {
                if let Some(txn_id) = *session_txn {
                    let tm = self.txn_manager.lock().unwrap();
                    if tm.is_active(txn_id) {
                        let undo_pos = if let Some(log) = tm.get_undo_log_ref(txn_id) {
                            log.len()
                        } else {
                            0
                        };
                        drop(tm);
                        let mut sp = self.savepoints.lock().unwrap();
                        sp.entry(txn_id).or_default().push((name.clone(), undo_pos));
                    }
                }
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                });
            }
            Statement::RollbackTo { ref name } => {
                if let Some(txn_id) = *session_txn {
                    let undo_pos = {
                        let mut sp = self.savepoints.lock().unwrap();
                        if let Some(list) = sp.get_mut(&txn_id) {
                            if let Some(pos) = list.iter().rposition(|(n, _)| n == name) {
                                let (_, undo_pos) = list[pos].clone();
                                list.truncate(pos + 1);
                                Some(undo_pos)
                            } else { None }
                        } else { None }
                    };
                    if let Some(undo_pos) = undo_pos {
                        // Take undo log, apply entries after the savepoint position, put back the rest
                        let mut tm = self.txn_manager.lock().unwrap();
                        if let Some(mut full_log) = tm.take_undo_log(txn_id) {
                            let all_entries = full_log.into_entries();
                            if undo_pos < all_entries.len() {
                                // Entries after savepoint need to be undone
                                let (keep, undo_entries) = all_entries.split_at(undo_pos);
                                let undo_log = crate::txn::UndoLog::from_entries(undo_entries.to_vec());
                                // Re-insert the kept portion
                                let kept_log = crate::txn::UndoLog::from_entries(keep.to_vec());
                                tm.put_undo_log(txn_id, kept_log);
                                drop(tm);
                                // Apply undo for the entries after savepoint
                                let mut local_bpm = LocalBpm::new(&self.cbpm);
                                let catalog = self.catalog.read().unwrap();
                                let _ = crate::txn::undo::apply_undo(&undo_log, &mut local_bpm, &catalog);
                                drop(local_bpm);
                                drop(catalog);
                            } else {
                                // Nothing to undo — restore the log
                                let kept_log = crate::txn::UndoLog::from_entries(all_entries);
                                tm.put_undo_log(txn_id, kept_log);
                            }
                        }
                    }
                }
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                });
            }
            Statement::ReleaseSavepoint { ref name } => {
                if let Some(txn_id) = *session_txn {
                    let mut sp = self.savepoints.lock().unwrap();
                    if let Some(list) = sp.get_mut(&txn_id) {
                        list.retain(|(n, _)| n != name);
                    }
                }
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                });
            }
            _ => {}
        }

        // Phase 2b: Handle metadata queries and special statements
        if let Some(result) = self.try_handle_directly(&stmt)? {
            return Ok(result);
        }

        // Phase 2c: Resolve subqueries and CTEs in the statement
        let stmt = self.resolve_subqueries(stmt)?;

        // Phase 3: Plan (with cache for SELECT queries — skip parse+plan on cache hit)
        let trimmed_sql = sql_text.trim();
        let is_select = matches!(stmt, Statement::Select { .. });

        // Fast path: check plan cache before planning
        if is_select {
            let cache = self.plan_cache.read().unwrap();
            if let Some(cached) = cache.get(trimmed_sql) {
                let plan = cached.clone();
                drop(cache);
                return self.execute_plan_session(plan, session_txn);
            }
        }

        let plan = {
            let catalog = self.catalog.read().unwrap();
            let indexes = self.indexes.read().unwrap();
            let planner = Planner::new(&catalog, &indexes);
            planner.plan(stmt)?
        };

        // Store in plan cache for SELECT queries
        if is_select {
            let mut cache = self.plan_cache.write().unwrap();
            if cache.len() >= 10000 { cache.clear(); }
            cache.insert(trimmed_sql.to_string(), plan.clone());
        }

        // Phase 4: Execute with session transaction awareness
        let result = self.execute_plan_session(plan, session_txn)?;

        // Phase 5: Fire AFTER triggers for DML
        if !is_select {
            self.fire_triggers_if_needed(sql_text, session_txn);
        }

        Ok(result)
    }

    fn execute_plan(&self, plan: PlanNode) -> Result<ExecuteResult> {
        self.execute_plan_session(plan, &mut None)
    }

    fn execute_plan_session(&self, plan: PlanNode, session_txn: &mut Option<TxnId>) -> Result<ExecuteResult> {
        let is_read_only = matches!(
            plan,
            PlanNode::SeqScan { .. }
            | PlanNode::IndexScan { .. }
            | PlanNode::Filter { .. }
            | PlanNode::Projection { .. }
            | PlanNode::NestedLoopJoin { .. }
            | PlanNode::HashJoin { .. }
            | PlanNode::Sort { .. }
            | PlanNode::Limit { .. }
            | PlanNode::GroupBy { .. }
            | PlanNode::Distinct { .. }
        );

        if is_read_only {
            // Zero-clone read path: borrow catalog/indexes directly via read locks.
            // No cloning needed — ReadContext holds immutable references.
            let catalog_guard = self.catalog.read().unwrap();
            let indexes_guard = self.indexes.read().unwrap();
            let clustered_guard = self.clustered_indexes.read().unwrap();

            // Build snapshot ONLY for explicit session transactions.
            // Auto-commit reads (session_txn == None) skip txn_manager entirely —
            // no lock contention, enabling full concurrency.
            let read_txn_ctx = if let Some(txn_id) = *session_txn {
                let tm = self.txn_manager.lock().unwrap();
                if tm.is_active(txn_id) {
                    let snapshot = tm.take_snapshot(txn_id);
                    Some(TxnContext { txn_id, snapshot, undo_log: UndoLog::new() })
                } else {
                    None
                }
            } else {
                // Auto-commit read: no snapshot needed, skip txn_manager lock
                None
            };

            let mut local_bpm = LocalBpm::new_readonly(&self.cbpm);
            let result = {
                let mut ctx = ReadContext {
                    bpm: &mut local_bpm,
                    catalog: &catalog_guard,
                    indexes: &indexes_guard,
                    clustered_indexes: &clustered_guard,
                    txn_ctx: read_txn_ctx,
                };
                executor::execute_read(plan, &mut ctx)
            };
            drop(local_bpm);
            drop(clustered_guard);
            drop(indexes_guard);
            drop(catalog_guard);
            result
        } else {
            // Determine if this is a DDL operation (needs catalog write lock)
            // or DML (INSERT/UPDATE/DELETE — only needs catalog read + index write).
            let is_ddl = matches!(
                plan,
                PlanNode::CreateTable { .. }
                | PlanNode::DropTable { .. }
                | PlanNode::CreateIndex { .. }
            );

            if is_ddl {
                // DDL: needs exclusive catalog access
                let mut catalog = self.catalog.write().unwrap();
                let mut indexes = self.indexes.write().unwrap();
                let mut clustered = self.clustered_indexes.write().unwrap();

                let mut local_bpm = LocalBpm::new(&self.cbpm);
                let result = {
                    let mut ctx = ExecutorContext {
                        bpm: &mut local_bpm,
                        catalog: &mut catalog,
                        indexes: &mut indexes,
                        clustered_indexes: &mut clustered,
                        auto_increment_counters: &self.auto_increment_counters,
                        txn_ctx: None,
                    };
                    executor::execute(plan, &mut ctx)?
                };
                drop(local_bpm);
                catalog.persist()?;
                // Invalidate plan cache on DDL changes
                self.invalidate_plan_cache();
                Ok(result)
            } else {
                // DML (INSERT/UPDATE/DELETE)
                // If inside a session transaction, use that; otherwise auto-transaction.
                let (txn_id, is_auto) = if let Some(stxn) = *session_txn {
                    (stxn, false)
                } else {
                    let mut tm = self.txn_manager.lock().unwrap();
                    // Use begin_fast for auto-transactions: skips WAL Begin record.
                    // MVCC visibility handles crash safety (uncommitted xmin is invisible).
                    (tm.begin_fast()?, true)
                };

                // Use READ locks on indexes/clustered_indexes for DML.
                let catalog_guard = self.catalog.read().unwrap();
                let indexes = self.indexes.read().unwrap();
                let clustered = self.clustered_indexes.read().unwrap();

                // Acquire per-table exclusive lock via LockManager.
                // Two DMLs on DIFFERENT tables run concurrently.
                // Two DMLs on the SAME table serialize (prevents lost updates).
                let table_name = Self::extract_table_from_plan(&plan);
                if let Some(ref tname) = table_name {
                    let target = crate::txn::LockTarget { table: tname.clone(), key: "__table__".into() };
                    let _ = self.lock_manager.acquire(txn_id, &target, crate::txn::LockMode::Exclusive);
                }

                // Build TxnContext AFTER locks: snapshot is consistent.
                let txn_ctx = {
                    let tm = self.txn_manager.lock().unwrap();
                    let snapshot = tm.take_snapshot(txn_id);
                    TxnContext { txn_id, snapshot, undo_log: UndoLog::new() }
                };
                let mut local_bpm = LocalBpm::new(&self.cbpm);
                let mut ctx = DmlContext {
                    bpm: &mut local_bpm,
                    catalog: &*catalog_guard,
                    indexes: &indexes,
                    clustered_indexes: &clustered,
                    auto_increment_counters: &self.auto_increment_counters,
                    txn_ctx: Some(txn_ctx),
                };
                let exec_result = executor::execute_dml(plan, &mut ctx);

                // Extract undo log from context
                let undo_log = ctx.txn_ctx.take().map(|tc| tc.undo_log);

                // Capture before-images for WAL
                let wal_entries = local_bpm.drain_wal_entries(txn_id);
                drop(local_bpm);

                match exec_result {
                    Ok(result) => {
                        let mut tm = self.txn_manager.lock().unwrap();
                        // Store undo log entries back in TxnManager
                        if let Some(log) = undo_log {
                            if !log.is_empty() {
                                // get_undo_log works because begin() created the entry
                                // and we didn't call take_undo_log
                                if let Some(existing) = tm.get_undo_log(txn_id) {
                                    for entry in log.into_entries() {
                                        existing.push(entry);
                                    }
                                }
                            }
                        }
                        if is_auto {
                            // Auto-transaction: commit_fast skips WAL Commit record.
                            // Dirty pages in the buffer pool provide crash safety — on recovery,
                            // uncommitted changes are invisible via MVCC (xmin not committed).
                            // WAL page images are only written on explicit COMMIT for session
                            // transactions, or on checkpoint/shutdown for full durability.
                            tm.commit_fast(txn_id)?;
                            drop(tm);
                            self.lock_manager.release_all(txn_id);
                        } else {
                            // Session transaction: batch WAL entries for commit-time write
                            drop(tm);
                            if !wal_entries.is_empty() {
                                let mut pending = self.pending_wal.lock().unwrap();
                                pending.entry(txn_id).or_default().extend(wal_entries);
                            }
                        }
                        Ok(result)
                    }
                    Err(e) => {
                        // On error, apply undo to reverse any partial DML
                        if let Some(ref log) = undo_log {
                            if !log.is_empty() {
                                let mut local_bpm = LocalBpm::new(&self.cbpm);
                                let catalog = self.catalog.read().unwrap();
                                let _ = crate::txn::undo::apply_undo(log, &mut local_bpm, &catalog);
                                drop(local_bpm);
                                drop(catalog);
                            }
                        }
                        let mut tm = self.txn_manager.lock().unwrap();
                        if is_auto {
                            let _ = tm.abort_fast(txn_id);
                            drop(tm);
                            self.lock_manager.release_all(txn_id);
                        } else {
                            let _ = tm.abort(txn_id);
                            drop(tm);
                            self.lock_manager.release_all(txn_id);
                            *session_txn = None;
                        }
                        Err(e)
                    }
                }
            }
        }
    }

    /// Handle metadata/session statements and new statement types.
    fn try_handle_directly(&self, stmt: &Statement) -> Result<Option<ExecuteResult>> {
        match stmt {
            Statement::ShowTables => {
                let catalog = self.catalog.read().unwrap();
                let tables = catalog.list_tables();
                let mut rows: Vec<Vec<Value>> = tables
                    .iter()
                    .map(|t| vec![Value::Varchar(t.name.clone())])
                    .collect();
                rows.sort_by(|a, b| a[0].compare(&b[0]).unwrap_or(std::cmp::Ordering::Equal));
                Ok(Some(ExecuteResult {
                    columns: vec!["Tables_in_forgedb".to_string()],
                    rows,
                    rows_affected: 0, last_insert_id: 0, message: String::new(),
                }))
            }
            Statement::ShowColumns { table_name } => {
                let catalog = self.catalog.read().unwrap();
                let info = catalog.get_table(table_name).ok_or_else(|| {
                    ForgeError::Execution(format!("table '{}' not found", table_name))
                })?;
                let rows: Vec<Vec<Value>> = info.schema.columns.iter()
                    .map(|c| vec![
                        Value::Varchar(c.name.clone()),
                        Value::Varchar(format!("{:?}", c.data_type)),
                        Value::Varchar(if c.nullable { "YES" } else { "NO" }.into()),
                        Value::Varchar(if c.is_primary_key { "PRI" } else { "" }.into()),
                        Value::Null,
                        Value::Varchar(if c.auto_increment { "auto_increment" } else { "" }.into()),
                    ])
                    .collect();
                Ok(Some(ExecuteResult {
                    columns: vec!["Field".into(), "Type".into(), "Null".into(), "Key".into(), "Default".into(), "Extra".into()],
                    rows,
                    rows_affected: 0, last_insert_id: 0, message: String::new(),
                }))
            }
            Statement::DescribeTable { table_name } => {
                self.try_handle_directly(&Statement::ShowColumns { table_name: table_name.clone() })
            }
            Statement::ShowCreateTable { table_name } => {
                let catalog = self.catalog.read().unwrap();
                let info = catalog.get_table(table_name).ok_or_else(|| {
                    ForgeError::Execution(format!("table '{}' not found", table_name))
                })?;
                let mut ddl = format!("CREATE TABLE `{}` (\n", info.name);
                for (i, col) in info.schema.columns.iter().enumerate() {
                    if i > 0 { ddl.push_str(",\n"); }
                    ddl.push_str(&format!("  `{}` {:?}{}{}", col.name, col.data_type,
                        if col.nullable { " NULL" } else { " NOT NULL" },
                        if col.auto_increment { " AUTO_INCREMENT" } else { "" }));
                }
                ddl.push_str("\n)");
                Ok(Some(ExecuteResult {
                    columns: vec!["Table".into(), "Create Table".into()],
                    rows: vec![vec![Value::Varchar(info.name.clone()), Value::Varchar(ddl)]],
                    rows_affected: 0, last_insert_id: 0, message: String::new(),
                }))
            }
            Statement::SetVariable { .. } | Statement::UseDatabase { .. } => {
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // Transaction control is handled in execute_sql_session
            Statement::StartTransaction | Statement::Commit | Statement::Rollback => {
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // ANALYZE TABLE — gather statistics for cost-based optimizer
            Statement::AnalyzeTable { table_name } => {
                self.analyze_table(table_name)?;
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // EXPLAIN
            Statement::Explain { statement } => {
                let plan = {
                    let catalog = self.catalog.read().unwrap();
                    let indexes = self.indexes.read().unwrap();
                    let planner = Planner::new(&catalog, &indexes);
                    planner.plan(*statement.clone())?
                };
                let plan_text = format!("{}", plan);
                Ok(Some(ExecuteResult {
                    columns: vec!["Query Plan".into()],
                    rows: plan_text.lines()
                        .filter(|l| !l.is_empty())
                        .map(|l| vec![Value::Varchar(l.to_string())])
                        .collect(),
                    rows_affected: 0, last_insert_id: 0, message: String::new(),
                }))
            }

            // UNION / UNION ALL
            Statement::Union { left, right, all } => {
                let left_result = self.execute_stmt_internal(left)?;
                let right_result = self.execute_stmt_internal(right)?;
                let columns = if !left_result.columns.is_empty() {
                    left_result.columns
                } else {
                    right_result.columns
                };
                let mut rows = left_result.rows;
                if *all {
                    rows.extend(right_result.rows);
                } else {
                    // UNION (distinct) - deduplicate using compact binary keys
                    let mut seen = std::collections::HashSet::new();
                    let mut deduped = Vec::new();
                    for row in rows {
                        let key = crate::executor::aggregate::serialize_row_key(&row);
                        if seen.insert(key) {
                            deduped.push(row);
                        }
                    }
                    for row in right_result.rows {
                        let key = crate::executor::aggregate::serialize_row_key(&row);
                        if seen.insert(key) {
                            deduped.push(row);
                        }
                    }
                    rows = deduped;
                }
                Ok(Some(ExecuteResult {
                    columns, rows,
                    rows_affected: 0, last_insert_id: 0, message: String::new(),
                }))
            }

            // CREATE VIEW
            Statement::CreateView { name, column_aliases, query } => {
                // Store the original SQL representation
                let sql = format!("{:?}", query);
                let mut catalog = self.catalog.write().unwrap();
                catalog.create_view(name, sql, column_aliases.clone())?;
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // DROP VIEW
            Statement::DropView { name, if_exists } => {
                let mut catalog = self.catalog.write().unwrap();
                if *if_exists && catalog.get_view(name).is_none() {
                    return Ok(Some(ExecuteResult {
                        rows: vec![], columns: vec![],
                        rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                    }));
                }
                catalog.drop_view(name)?;
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // TRUNCATE TABLE
            Statement::TruncateTable { table_name } => {
                // Delete all rows: scan and delete each one
                let delete_result = self.execute_stmt_internal(
                    &Statement::Delete {
                        table_name: table_name.clone(),
                        r#where: None,
                    },
                )?;
                // Reset auto-increment counters for this table
                let mut auto_inc = self.auto_increment_counters.lock().unwrap();
                auto_inc.retain(|k, _| !k.starts_with(&format!("{}.", table_name.to_lowercase())));
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: delete_result.rows_affected,
                    last_insert_id: 0,
                    message: "OK".into(),
                }))
            }

            // INSERT ... SELECT
            Statement::InsertSelect { table_name, columns, query } => {
                // Execute the SELECT query
                let select_result = self.execute_stmt_internal(query)?;
                // Build INSERT values from the result
                let values: Vec<Vec<Expr>> = select_result.rows.iter().map(|row| {
                    row.iter().map(|v| value_to_literal_expr(v)).collect()
                }).collect();
                if values.is_empty() {
                    return Ok(Some(ExecuteResult {
                        rows: vec![], columns: vec![],
                        rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                    }));
                }
                // Execute as a regular INSERT
                let insert_stmt = Statement::Insert {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    values,
                    on_conflict: None,
                };
                let result = self.execute_stmt_internal(&insert_stmt)?;
                Ok(Some(result))
            }

            // CREATE SEQUENCE
            Statement::CreateSequence { name, start, increment } => {
                let mut seqs = self.sequences.lock().unwrap();
                let key = name.to_lowercase();
                seqs.insert(key, (*start - *increment, *increment)); // pre-decrement so first NEXTVAL returns start
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // CREATE DATABASE / DROP DATABASE / USE DATABASE
            Statement::CreateDatabase { .. } | Statement::DropDatabase { .. } => {
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // SAVEPOINT / ROLLBACK TO / RELEASE SAVEPOINT (handled in execute_sql_session)
            Statement::Savepoint { .. }
            | Statement::RollbackTo { .. }
            | Statement::ReleaseSavepoint { .. } => {
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // ALTER TABLE
            Statement::AlterTable { table_name, operations } => {
                let mut catalog = self.catalog.write().unwrap();
                for op in operations {
                    match op {
                        AlterTableOp::AddColumn(col_def) => {
                            catalog.alter_table_schema(table_name, |schema| {
                                let col_id = schema.columns.len() as u16;
                                schema.columns.push(Column {
                                    name: col_def.name.clone(),
                                    data_type: col_def.data_type.clone(),
                                    nullable: col_def.nullable,
                                    column_id: col_id,
                                    auto_increment: col_def.auto_increment,
                                    default_value: None,
                                    is_primary_key: col_def.is_primary_key,
                                    is_unique: false,
                                    check_expr: None, fk_ref: None,
                                });
                                Ok(())
                            })?;
                        }
                        AlterTableOp::DropColumn(col_name) => {
                            catalog.alter_table_schema(table_name, |schema| {
                                schema.columns.retain(|c| !c.name.eq_ignore_ascii_case(col_name));
                                Ok(())
                            })?;
                        }
                        AlterTableOp::ModifyColumn(col_def) => {
                            catalog.alter_table_schema(table_name, |schema| {
                                if let Some(col) = schema.columns.iter_mut()
                                    .find(|c| c.name.eq_ignore_ascii_case(&col_def.name))
                                {
                                    col.data_type = col_def.data_type.clone();
                                    col.nullable = col_def.nullable;
                                }
                                Ok(())
                            })?;
                        }
                        AlterTableOp::RenameColumn { old_name, new_name } => {
                            catalog.alter_table_schema(table_name, |schema| {
                                if let Some(col) = schema.columns.iter_mut()
                                    .find(|c| c.name.eq_ignore_ascii_case(old_name))
                                {
                                    col.name = new_name.clone();
                                }
                                Ok(())
                            })?;
                        }
                        AlterTableOp::AddIndex { .. } => {
                            // Index creation handled separately
                        }
                    }
                }
                catalog.persist()?;
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            // Stored procedures & triggers (handled in execute_sql_session pre-parse)
            Statement::CreateProcedure { .. }
            | Statement::ExecProcedure { .. }
            | Statement::CreateTrigger { .. }
            | Statement::CreateUser { .. }
            | Statement::DropUser { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. }
            | Statement::Prepare { .. }
            | Statement::ExecutePrepared { .. }
            | Statement::Backup { .. }
            | Statement::Restore { .. }
            | Statement::DeclareCursor { .. }
            | Statement::OpenCursor { .. }
            | Statement::FetchCursor { .. }
            | Statement::CloseCursor { .. }
            | Statement::DeallocateCursor { .. } => {
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }

            _ => Ok(None),
        }
    }

    /// Execute a statement that's already parsed (used for UNION sub-queries).
    fn execute_stmt_internal(&self, stmt: &Statement) -> Result<ExecuteResult> {
        if let Some(result) = self.try_handle_directly(stmt)? {
            return Ok(result);
        }

        let plan = {
            let catalog = self.catalog.read().unwrap();
            let indexes = self.indexes.read().unwrap();
            let planner = Planner::new(&catalog, &indexes);
            planner.plan(stmt.clone())?
        };

        self.execute_plan(plan)
    }

    /// Gather statistics for a table (ANALYZE TABLE).
    fn analyze_table(&self, table_name: &str) -> Result<()> {
        // First, scan the table to gather data
        let (row_count, col_data) = {
            let catalog = self.catalog.read().unwrap();
            let info = catalog.get_table(table_name).ok_or_else(|| {
                ForgeError::Execution(format!("table '{}' not found", table_name))
            })?;
            let schema = info.schema.clone();
            let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

            let mut local_bpm = LocalBpm::new(&self.cbpm);
            let clustered = self.clustered_indexes.read().unwrap();

            // Scan all rows
            let rows = if let Some(cidx) = clustered.get(&table_name.to_lowercase()) {
                let raw_rows = cidx.scan_all(&mut local_bpm)?;
                let mut rows = Vec::with_capacity(raw_rows.len());
                for raw in raw_rows {
                    let values = crate::tuple::tuple::deserialize(&raw, &schema)?;
                    rows.push(values);
                }
                rows
            } else {
                let (_schema, rid_rows) = crate::executor::seq_scan::execute_seq_scan(
                    table_name, &mut local_bpm, &catalog,
                )?;
                rid_rows.into_iter().map(|(_, v)| v).collect()
            };

            let row_count = rows.len() as u64;

            // Compute per-column statistics
            let mut col_data: Vec<(String, ColumnStatistics)> = Vec::new();
            for (ci, col_name) in col_names.iter().enumerate() {
                let mut distinct_set: HashSet<Vec<u8>> = HashSet::new();
                let mut null_count = 0u64;
                let mut min_val: Option<Value> = None;
                let mut max_val: Option<Value> = None;

                for row in &rows {
                    if ci < row.len() {
                        let val = &row[ci];
                        if val.is_null() {
                            null_count += 1;
                        } else {
                            distinct_set.insert(crate::executor::aggregate::serialize_row_key(&[val.clone()]));
                            match &min_val {
                                None => min_val = Some(val.clone()),
                                Some(cur) => {
                                    if let Some(std::cmp::Ordering::Less) = val.compare(cur) {
                                        min_val = Some(val.clone());
                                    }
                                }
                            }
                            match &max_val {
                                None => max_val = Some(val.clone()),
                                Some(cur) => {
                                    if let Some(std::cmp::Ordering::Greater) = val.compare(cur) {
                                        max_val = Some(val.clone());
                                    }
                                }
                            }
                        }
                    }
                }

                col_data.push((
                    col_name.to_lowercase(),
                    ColumnStatistics {
                        distinct_count: distinct_set.len() as u64,
                        null_count,
                        min_value: min_val,
                        max_value: max_val,
                    },
                ));
            }

            (row_count, col_data)
        };

        // Now update the catalog with statistics
        let mut catalog = self.catalog.write().unwrap();
        let key = table_name.to_lowercase();
        if let Some(info) = catalog.tables.get_mut(&key) {
            // Estimate page count: ~100 rows per page for typical row sizes
            let page_count = (row_count / 100).max(1);
            info.stats = Some(TableStatistics {
                row_count,
                page_count,
            });
            info.column_stats.clear();
            for (col_name, stats) in col_data {
                info.column_stats.insert(col_name, stats);
            }
        }

        Ok(())
    }

    /// Resolve subqueries in a statement by executing them and inlining results.
    /// Also resolves CTEs by converting them into derived tables.
    fn has_subqueries_in_expr(expr: &Option<Expr>) -> bool {
        match expr {
            None => false,
            Some(e) => Self::expr_has_subquery(e),
        }
    }

    fn expr_has_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => true,
            Expr::BinaryOp { left, right, .. } => Self::expr_has_subquery(left) || Self::expr_has_subquery(right),
            Expr::UnaryOp { expr, .. } => Self::expr_has_subquery(expr),
            Expr::IsNull(e) | Expr::IsNotNull(e) => Self::expr_has_subquery(e),
            Expr::Between { expr, low, high, .. } => Self::expr_has_subquery(expr) || Self::expr_has_subquery(low) || Self::expr_has_subquery(high),
            Expr::Case { operand, when_clauses, else_result } => {
                operand.as_ref().map_or(false, |e| Self::expr_has_subquery(e))
                    || when_clauses.iter().any(|(w, t)| Self::expr_has_subquery(w) || Self::expr_has_subquery(t))
                    || else_result.as_ref().map_or(false, |e| Self::expr_has_subquery(e))
            }
            Expr::In { expr, list, .. } => Self::expr_has_subquery(expr) || list.iter().any(|e| Self::expr_has_subquery(e)),
            Expr::NotIn { expr, list, .. } => Self::expr_has_subquery(expr) || list.iter().any(|e| Self::expr_has_subquery(e)),
            _ => false,
        }
    }

    fn has_derived_table(from: &crate::sql::ast::FromClause) -> bool {
        matches!(from, crate::sql::ast::FromClause::Subquery { .. })
            || matches!(from, crate::sql::ast::FromClause::Join { left, right, .. }
                if Self::has_derived_table(left) || Self::has_derived_table(right))
    }

    fn has_subqueries_in_columns(cols: &[crate::sql::ast::SelectColumn]) -> bool {
        cols.iter().any(|c| match c {
            crate::sql::ast::SelectColumn::Expr { expr, .. } => Self::expr_has_subquery(expr),
            _ => false,
        })
    }

    fn resolve_subqueries(&self, stmt: Statement) -> Result<Statement> {
        match stmt {
            Statement::Select { ref r#where, ref having, ref from, ref columns, ref ctes, .. } if
                ctes.is_empty() && !Self::has_subqueries_in_expr(r#where) && !Self::has_subqueries_in_expr(having)
                && !Self::has_derived_table(from) && !Self::has_subqueries_in_columns(columns) =>
            {
                // Fast path: no CTEs, no subqueries, no derived tables — return as-is
                return Ok(stmt);
            }
            Statement::Select { distinct, columns, from, r#where, group_by, having, order_by, limit, offset, ctes } => {
                // Resolve recursive CTEs first
                let mut cte_map: HashMap<String, Statement> = HashMap::new();
                for cte in &ctes {
                    if cte.recursive {
                        // Execute recursive CTE iteratively
                        let resolved = self.execute_recursive_cte(cte)?;
                        cte_map.insert(cte.name.to_lowercase(), resolved);
                    } else {
                        cte_map.insert(cte.name.to_lowercase(), *cte.query.clone());
                    }
                }

                // Resolve FROM clause CTE references
                let from = self.resolve_from_ctes(from, &cte_map)?;

                // Materialize derived tables (subqueries in FROM)
                let from = self.materialize_derived_tables(from)?;

                // Resolve subqueries in WHERE clause
                let r#where = if let Some(expr) = r#where {
                    Some(self.resolve_expr_subqueries(expr)?)
                } else {
                    None
                };
                // Resolve subqueries in HAVING clause
                let having = if let Some(expr) = having {
                    Some(self.resolve_expr_subqueries(expr)?)
                } else {
                    None
                };
                // Resolve window functions in columns
                let columns = columns.into_iter().map(|c| {
                    match c {
                        crate::sql::ast::SelectColumn::Expr { expr, alias } => {
                            let resolved = self.resolve_expr_subqueries(expr).unwrap_or_else(|_| Expr::Literal(LiteralValue::Null));
                            crate::sql::ast::SelectColumn::Expr { expr: resolved, alias }
                        }
                        other => other,
                    }
                }).collect();
                Ok(Statement::Select { distinct, columns, from, r#where, group_by, having, order_by, limit, offset, ctes: vec![] })
            }
            Statement::Update { table_name, assignments, r#where } => {
                let r#where = if let Some(expr) = r#where {
                    Some(self.resolve_expr_subqueries(expr)?)
                } else {
                    None
                };
                Ok(Statement::Update { table_name, assignments, r#where })
            }
            Statement::Delete { table_name, r#where } => {
                let r#where = if let Some(expr) = r#where {
                    Some(self.resolve_expr_subqueries(expr)?)
                } else {
                    None
                };
                Ok(Statement::Delete { table_name, r#where })
            }
            other => Ok(other),
        }
    }

    /// Resolve CTE references in FROM clause
    fn resolve_from_ctes(&self, from: crate::sql::ast::FromClause, cte_map: &HashMap<String, Statement>) -> Result<crate::sql::ast::FromClause> {
        match from {
            crate::sql::ast::FromClause::Table { ref name, ref alias } => {
                if let Some(cte_query) = cte_map.get(&name.to_lowercase()) {
                    Ok(crate::sql::ast::FromClause::Subquery {
                        query: Box::new(cte_query.clone()),
                        alias: alias.clone().unwrap_or_else(|| name.clone()),
                    })
                } else {
                    Ok(from)
                }
            }
            crate::sql::ast::FromClause::Join { left, right, join_type, on } => {
                let left = self.resolve_from_ctes(*left, cte_map)?;
                let right = self.resolve_from_ctes(*right, cte_map)?;
                Ok(crate::sql::ast::FromClause::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    on,
                })
            }
            other => Ok(other),
        }
    }

    /// Materialize derived tables (FROM subqueries) into temp tables.
    fn materialize_derived_tables(&self, from: crate::sql::ast::FromClause) -> Result<crate::sql::ast::FromClause> {
        use crate::sql::ast::FromClause;
        match from {
            FromClause::Subquery { query, alias } => {
                // Execute the subquery and create a temp table
                let result = self.execute_stmt_internal(&query)?;
                let temp_name = format!("__derived_{}__", alias);

                // Build CREATE TABLE + INSERT from results
                let mut col_defs = Vec::new();
                for (i, col_name) in result.columns.iter().enumerate() {
                    let dt = if let Some(first_row) = result.rows.first() {
                        if i < first_row.len() {
                            first_row[i].data_type().unwrap_or(crate::tuple::types::DataType::Varchar(255))
                        } else {
                            crate::tuple::types::DataType::Varchar(255)
                        }
                    } else {
                        crate::tuple::types::DataType::Varchar(255)
                    };
                    col_defs.push(crate::sql::ast::ColumnDef {
                        name: col_name.clone(),
                        data_type: dt,
                        nullable: true,
                        auto_increment: false,
                        default_value: None,
                        is_primary_key: false,
                        is_unique: false,
                        check_expr: None,
                        references: None,
                    });
                }

                // Create temp table
                let create = Statement::CreateTable {
                    table_name: temp_name.clone(),
                    columns: col_defs,
                    if_not_exists: true,
                };
                self.execute_stmt_internal(&create)?;
                // Clear any existing data
                let _ = self.execute_stmt_internal(&Statement::Delete {
                    table_name: temp_name.clone(),
                    r#where: None,
                });

                // Insert rows
                if !result.rows.is_empty() {
                    let values: Vec<Vec<Expr>> = result.rows.iter().map(|row| {
                        row.iter().map(|v| value_to_literal_expr(v)).collect()
                    }).collect();
                    let insert = Statement::Insert {
                        table_name: temp_name.clone(),
                        columns: None,
                        values,
                        on_conflict: None,
                    };
                    self.execute_stmt_internal(&insert)?;
                }

                Ok(FromClause::Table { name: temp_name, alias: Some(alias) })
            }
            FromClause::Join { left, right, join_type, on } => {
                let left = self.materialize_derived_tables(*left)?;
                let right = self.materialize_derived_tables(*right)?;
                Ok(FromClause::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    on,
                })
            }
            other => Ok(other),
        }
    }

    /// Resolve subquery expressions by executing them and inlining results.
    fn resolve_expr_subqueries(&self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::InSubquery { expr: inner_expr, subquery, negated } => {
                // Execute the subquery once
                let result = self.execute_stmt_internal(&subquery)?;
                // Collect values and build HashSet for O(1) lookup
                let values: Vec<Value> = result.rows.iter().filter_map(|row| {
                    if row.is_empty() { None } else { Some(row[0].clone()) }
                }).collect();
                let keys: std::collections::HashSet<Vec<u8>> = values.iter()
                    .filter(|v| !v.is_null())
                    .map(|v| v.to_sort_key_bytes())
                    .collect();
                let resolved_inner = self.resolve_expr_subqueries(*inner_expr)?;
                Ok(Expr::InValues {
                    expr: Box::new(resolved_inner),
                    values,
                    keys,
                    negated,
                })
            }
            Expr::Exists { subquery, negated } => {
                // Execute the subquery
                let result = self.execute_stmt_internal(&subquery)?;
                let exists = !result.rows.is_empty();
                let val = if negated { !exists } else { exists };
                Ok(Expr::Literal(LiteralValue::Boolean(val)))
            }
            Expr::Subquery(subquery) => {
                // Execute the scalar subquery
                let result = self.execute_stmt_internal(&subquery)?;
                if result.rows.is_empty() || result.rows[0].is_empty() {
                    Ok(Expr::Literal(LiteralValue::Null))
                } else {
                    Ok(value_to_literal_expr(&result.rows[0][0]))
                }
            }
            // Recurse into compound expressions
            Expr::BinaryOp { left, op, right } => {
                let left = self.resolve_expr_subqueries(*left)?;
                let right = self.resolve_expr_subqueries(*right)?;
                Ok(Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let inner = self.resolve_expr_subqueries(*inner)?;
                Ok(Expr::UnaryOp { op, expr: Box::new(inner) })
            }
            Expr::IsNull(inner) => {
                let inner = self.resolve_expr_subqueries(*inner)?;
                Ok(Expr::IsNull(Box::new(inner)))
            }
            Expr::IsNotNull(inner) => {
                let inner = self.resolve_expr_subqueries(*inner)?;
                Ok(Expr::IsNotNull(Box::new(inner)))
            }
            Expr::Function { ref name, ref args, .. } => {
                // Handle NEXTVAL('sequence_name')
                let upper = name.to_uppercase();
                if upper == "NEXTVAL" && args.len() == 1 {
                    if let Expr::Literal(LiteralValue::String(ref seq_name)) = args[0] {
                        let mut seqs = self.sequences.lock().unwrap();
                        let key = seq_name.to_lowercase();
                        if let Some((ref mut current, increment)) = seqs.get_mut(&key) {
                            *current += *increment;
                            let val = *current;
                            return Ok(Expr::Literal(LiteralValue::Integer(val)));
                        }
                        return Err(ForgeError::Execution(format!("sequence '{}' not found", seq_name)));
                    }
                }
                Ok(expr)
            }
            // Window functions are kept as-is (resolved during execution)
            other => Ok(other),
        }
    }

    /// Execute a recursive CTE iteratively until fixed point.
    /// The CTE query must be a UNION ALL of base case and recursive case.
    fn execute_recursive_cte(&self, cte: &crate::sql::ast::Cte) -> Result<Statement> {
        use crate::sql::ast::{SelectColumn, FromClause};

        // Execute the CTE query as a union: first get the base result
        let base_result = self.execute_stmt_internal(&cte.query)?;
        let columns_names = base_result.columns.clone();

        let mut all_rows = base_result.rows.clone();
        let mut working_set = base_result.rows;

        // Iterate: execute the recursive part with the working set as the CTE table
        // Limit iterations to prevent infinite loops
        let max_iterations = 1000;
        for _ in 0..max_iterations {
            if working_set.is_empty() {
                break;
            }

            // Create a temp table with the working set
            let temp_name = format!("__recursive_cte_{}__", cte.name);

            // Build column defs from working set
            let mut col_defs = Vec::new();
            for (i, col_name) in columns_names.iter().enumerate() {
                let dt = if let Some(first_row) = working_set.first() {
                    if i < first_row.len() {
                        first_row[i].data_type().unwrap_or(crate::tuple::types::DataType::Varchar(255))
                    } else {
                        crate::tuple::types::DataType::Varchar(255)
                    }
                } else {
                    crate::tuple::types::DataType::Varchar(255)
                };
                col_defs.push(crate::sql::ast::ColumnDef {
                    name: col_name.clone(),
                    data_type: dt,
                    nullable: true,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                    is_unique: false,
                    check_expr: None,
                    references: None,
                });
            }

            let create = Statement::CreateTable {
                table_name: temp_name.clone(),
                columns: col_defs,
                if_not_exists: true,
            };
            let _ = self.execute_stmt_internal(&create);
            // Clear old data
            let _ = self.execute_stmt_internal(&Statement::Delete {
                table_name: temp_name.clone(),
                r#where: None,
            });

            // Insert working set into temp table
            if !working_set.is_empty() {
                let values: Vec<Vec<Expr>> = working_set.iter().map(|row| {
                    row.iter().map(|v| value_to_literal_expr(v)).collect()
                }).collect();
                let insert = Statement::Insert {
                    table_name: temp_name.clone(),
                    columns: None,
                    values,
                    on_conflict: None,
                };
                let _ = self.execute_stmt_internal(&insert);
            }

            // Re-execute the query (which references the CTE name, now mapped to temp table)
            let new_result = self.execute_stmt_internal(&cte.query);
            match new_result {
                Ok(result) => {
                    // The new rows are just the ones not already in all_rows
                    let new_rows: Vec<Vec<Value>> = result.rows.into_iter()
                        .filter(|row| !all_rows.contains(row))
                        .collect();
                    if new_rows.is_empty() {
                        break;
                    }
                    all_rows.extend(new_rows.clone());
                    working_set = new_rows;
                }
                Err(_) => break,
            }

            // Clean up temp table
            let _ = self.execute_stmt_internal(&Statement::DropTable {
                table_name: temp_name,
                if_exists: true,
            });
        }

        // Build a UNION ALL of all accumulated rows as literal selects
        // For simplicity, create a materialized temp table result
        let temp_result_name = format!("__recursive_result_{}__", cte.name);
        let mut col_defs = Vec::new();
        for (i, col_name) in columns_names.iter().enumerate() {
            let dt = if let Some(first_row) = all_rows.first() {
                if i < first_row.len() {
                    first_row[i].data_type().unwrap_or(crate::tuple::types::DataType::Varchar(255))
                } else {
                    crate::tuple::types::DataType::Varchar(255)
                }
            } else {
                crate::tuple::types::DataType::Varchar(255)
            };
            col_defs.push(crate::sql::ast::ColumnDef {
                name: col_name.clone(),
                data_type: dt,
                nullable: true,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                is_unique: false,
                check_expr: None,
                references: None,
            });
        }

        let create = Statement::CreateTable {
            table_name: temp_result_name.clone(),
            columns: col_defs,
            if_not_exists: true,
        };
        let _ = self.execute_stmt_internal(&create);
        let _ = self.execute_stmt_internal(&Statement::Delete {
            table_name: temp_result_name.clone(),
            r#where: None,
        });

        if !all_rows.is_empty() {
            let values: Vec<Vec<Expr>> = all_rows.iter().map(|row| {
                row.iter().map(|v| value_to_literal_expr(v)).collect()
            }).collect();
            let insert = Statement::Insert {
                table_name: temp_result_name.clone(),
                columns: None,
                values,
                on_conflict: None,
            };
            let _ = self.execute_stmt_internal(&insert);
        }

        // Return a SELECT * FROM temp_result_name
        Ok(Statement::Select {
            distinct: false,
            columns: vec![SelectColumn::AllColumns(None)],
            from: FromClause::Table { name: temp_result_name, alias: None },
            r#where: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            ctes: vec![],
        })
    }

    /// Abort a transaction by ID. Used for connection cleanup.
    pub fn abort_transaction(&self, txn_id: TxnId) {
        let mut tm = self.txn_manager.lock().unwrap();
        if tm.is_active(txn_id) {
            let _ = tm.abort(txn_id);
        }
        drop(tm);
        self.lock_manager.release_all(txn_id);
        self.savepoints.lock().unwrap().remove(&txn_id);
    }

    /// Get next value from a sequence.
    pub fn nextval(&self, seq_name: &str) -> Result<i64> {
        let mut seqs = self.sequences.lock().unwrap();
        let key = seq_name.to_lowercase();
        if let Some((ref mut current, increment)) = seqs.get_mut(&key) {
            *current += *increment;
            Ok(*current)
        } else {
            Err(ForgeError::Execution(format!("sequence '{}' not found", seq_name)))
        }
    }

    // -----------------------------------------------------------------------
    // Stored Procedures
    // -----------------------------------------------------------------------

    fn handle_create_procedure(&self, sql_text: &str) -> Result<ExecuteResult> {
        // Parse: CREATE PROCEDURE name [@param type, ...] AS BEGIN ... END
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // Extract procedure name
        let after_proc = if upper.starts_with("CREATE PROCEDURE ") {
            &text[17..]
        } else if upper.starts_with("CREATE PROC ") {
            &text[12..]
        } else {
            return Err(ForgeError::Parse("invalid CREATE PROCEDURE syntax".into()));
        };

        // Name is the first token
        let name = after_proc.split_whitespace().next().unwrap_or("").to_string();
        if name.is_empty() {
            return Err(ForgeError::Parse("missing procedure name".into()));
        }

        // Parse parameters (between name and AS)
        let params = Vec::new(); // Simplified: no params for now

        // Extract body between BEGIN and END (or after AS)
        let upper_after = after_proc.to_uppercase();
        let body_sql = if let Some(begin_pos) = upper_after.find("BEGIN") {
            let end_pos = upper_after.rfind("END").unwrap_or(upper_after.len());
            let body_text = &after_proc[begin_pos + 5..end_pos].trim();
            body_text.split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<String>>()
        } else if let Some(as_pos) = upper_after.find(" AS ") {
            let body_text = &after_proc[as_pos + 4..].trim();
            body_text.split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<String>>()
        } else {
            return Err(ForgeError::Parse("CREATE PROCEDURE: missing AS or BEGIN".into()));
        };

        let mut procs = self.procedures.lock().unwrap();
        procs.insert(name.to_lowercase(), (params, body_sql));

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    fn handle_exec_procedure(&self, rest: &str, session_txn: &mut Option<TxnId>) -> Result<ExecuteResult> {
        let parts: Vec<&str> = rest.splitn(2, |c: char| c.is_whitespace() || c == ';').collect();
        let proc_name = parts[0].trim_end_matches(';').to_lowercase();

        let body = {
            let procs = self.procedures.lock().unwrap();
            match procs.get(&proc_name) {
                Some((_, body)) => body.clone(),
                None => return Err(ForgeError::Execution(format!("procedure '{}' not found", proc_name))),
            }
        };

        let mut last_result = ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        };

        for stmt_sql in &body {
            last_result = self.execute_sql_session(stmt_sql, session_txn)?;
        }

        Ok(last_result)
    }

    // -----------------------------------------------------------------------
    // Triggers
    // -----------------------------------------------------------------------

    fn handle_create_trigger(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // CREATE TRIGGER name ON table AFTER|BEFORE INSERT|UPDATE|DELETE AS BEGIN ... END
        let after_ct = &text[15..]; // skip "CREATE TRIGGER "
        let name = after_ct.split_whitespace().next().unwrap_or("").to_string();

        let upper_rest = after_ct.to_uppercase();

        // Find table name after ON
        let table = if let Some(on_pos) = upper_rest.find(" ON ") {
            let after_on = &after_ct[on_pos + 4..].trim();
            after_on.split_whitespace().next().unwrap_or("").to_string()
        } else {
            return Err(ForgeError::Parse("CREATE TRIGGER: missing ON clause".into()));
        };

        // Determine event
        let event = if upper_rest.contains("AFTER INSERT") || upper_rest.contains("FOR INSERT") {
            TriggerEvent::AfterInsert
        } else if upper_rest.contains("AFTER UPDATE") || upper_rest.contains("FOR UPDATE") {
            TriggerEvent::AfterUpdate
        } else if upper_rest.contains("AFTER DELETE") || upper_rest.contains("FOR DELETE") {
            TriggerEvent::AfterDelete
        } else if upper_rest.contains("BEFORE INSERT") {
            TriggerEvent::BeforeInsert
        } else if upper_rest.contains("BEFORE UPDATE") {
            TriggerEvent::BeforeUpdate
        } else if upper_rest.contains("BEFORE DELETE") {
            TriggerEvent::BeforeDelete
        } else {
            TriggerEvent::AfterInsert // default
        };

        // Extract body
        let body = if let Some(begin_pos) = upper_rest.find("BEGIN") {
            let end_pos = upper_rest.rfind("END").unwrap_or(upper_rest.len());
            let body_text = &after_ct[begin_pos + 5..end_pos].trim();
            body_text.split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<String>>()
        } else if let Some(as_pos) = upper_rest.find(" AS ") {
            let body_text = &after_ct[as_pos + 4..].trim();
            body_text.split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<String>>()
        } else {
            vec![]
        };

        let trigger_def = TriggerDef {
            name,
            table: table.clone(),
            event,
            body,
        };

        let mut triggers = self.triggers.lock().unwrap();
        triggers.entry(table.to_lowercase())
            .or_insert_with(Vec::new)
            .push(trigger_def);

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    fn fire_triggers_if_needed(&self, sql_text: &str, session_txn: &mut Option<TxnId>) {
        let upper = sql_text.trim().to_uppercase();
        let (event, table_name) = if upper.starts_with("INSERT INTO ") || upper.starts_with("INSERT ") {
            let rest = if upper.starts_with("INSERT INTO ") { &upper[12..] } else { &upper[7..] };
            let table = rest.split_whitespace().next().unwrap_or("").to_string();
            (TriggerEvent::AfterInsert, table)
        } else if upper.starts_with("UPDATE ") {
            let table = upper[7..].split_whitespace().next().unwrap_or("").to_string();
            (TriggerEvent::AfterUpdate, table)
        } else if upper.starts_with("DELETE FROM ") || upper.starts_with("DELETE ") {
            let rest = if upper.starts_with("DELETE FROM ") { &upper[12..] } else { &upper[7..] };
            let table = rest.split_whitespace().next().unwrap_or("").to_string();
            (TriggerEvent::AfterDelete, table)
        } else {
            return;
        };

        let trigger_bodies: Vec<Vec<String>> = {
            let triggers = self.triggers.lock().unwrap();
            if let Some(trigs) = triggers.get(&table_name.to_lowercase()) {
                trigs.iter()
                    .filter(|t| t.event == event)
                    .map(|t| t.body.clone())
                    .collect()
            } else {
                vec![]
            }
        };

        for body in trigger_bodies {
            for stmt_sql in &body {
                let _ = self.execute_sql_session(stmt_sql, session_txn);
            }
        }
    }

    // -----------------------------------------------------------------------
    // User/Role Management
    // -----------------------------------------------------------------------

    fn handle_create_user(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // CREATE USER name WITH PASSWORD 'password'
        // or CREATE LOGIN name WITH PASSWORD = 'password'
        let after_keyword = if upper.starts_with("CREATE USER ") {
            &text[12..]
        } else {
            &text[13..] // CREATE LOGIN
        };

        let name = after_keyword.split_whitespace().next().unwrap_or("").to_string();
        let name = name.trim_matches('\'').trim_matches('"').to_string();

        // Extract password
        let password = if let Some(pw_pos) = upper.find("PASSWORD") {
            let after_pw = &text[pw_pos + 8..].trim();
            let after_pw = after_pw.trim_start_matches('=').trim_start_matches(' ');
            // Extract quoted string
            if after_pw.starts_with('\'') {
                let end = after_pw[1..].find('\'').unwrap_or(after_pw.len() - 1);
                after_pw[1..end + 1].to_string()
            } else {
                after_pw.split_whitespace().next().unwrap_or("").to_string()
            }
        } else {
            String::new()
        };

        let mut users = self.users.lock().unwrap();
        users.insert(name.to_lowercase(), (password, HashSet::new()));

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    fn handle_grant(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // GRANT privilege ON table TO user
        // GRANT ALL ON *.* TO user
        let after_grant = &upper[6..]; // skip "GRANT "
        let parts: Vec<&str> = after_grant.split_whitespace().collect();

        let privilege = parts.first().unwrap_or(&"ALL").to_string();
        let to_user = if let Some(to_pos) = parts.iter().position(|&p| p == "TO") {
            parts.get(to_pos + 1).unwrap_or(&"").trim_matches('\'').trim_matches('"').to_lowercase()
        } else {
            return Err(ForgeError::Parse("GRANT: missing TO clause".into()));
        };

        let on_table = if let Some(on_pos) = parts.iter().position(|&p| p == "ON") {
            Some(parts.get(on_pos + 1).unwrap_or(&"*").to_string())
        } else {
            None
        };

        let priv_key = format!("{}:{}", privilege, on_table.as_deref().unwrap_or("*"));
        let mut users = self.users.lock().unwrap();
        if let Some((_, privileges)) = users.get_mut(&to_user) {
            privileges.insert(priv_key);
        } else {
            // Auto-create user if not exists
            let mut privs = HashSet::new();
            privs.insert(priv_key);
            users.insert(to_user, (String::new(), privs));
        }

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    fn handle_revoke(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        let after_revoke = &upper[7..]; // skip "REVOKE "
        let parts: Vec<&str> = after_revoke.split_whitespace().collect();

        let privilege = parts.first().unwrap_or(&"ALL").to_string();
        let from_user = if let Some(from_pos) = parts.iter().position(|&p| p == "FROM") {
            parts.get(from_pos + 1).unwrap_or(&"").trim_matches('\'').trim_matches('"').to_lowercase()
        } else {
            return Err(ForgeError::Parse("REVOKE: missing FROM clause".into()));
        };

        let on_table = if let Some(on_pos) = parts.iter().position(|&p| p == "ON") {
            Some(parts.get(on_pos + 1).unwrap_or(&"*").to_string())
        } else {
            None
        };

        let priv_key = format!("{}:{}", privilege, on_table.as_deref().unwrap_or("*"));
        let mut users = self.users.lock().unwrap();
        if let Some((_, privileges)) = users.get_mut(&from_user) {
            privileges.remove(&priv_key);
        }

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    // -----------------------------------------------------------------------
    // Prepared Statements
    // -----------------------------------------------------------------------

    fn handle_prepare(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        // PREPARE name AS sql_text
        // or PREPARE name FROM 'sql_text'
        let after_prepare = &text[8..]; // skip "PREPARE "
        let name = after_prepare.split_whitespace().next().unwrap_or("").to_string();

        let upper_rest = after_prepare.to_uppercase();
        let sql = if let Some(as_pos) = upper_rest.find(" AS ") {
            after_prepare[as_pos + 4..].trim().to_string()
        } else if let Some(from_pos) = upper_rest.find(" FROM ") {
            let s = after_prepare[from_pos + 6..].trim();
            s.trim_matches('\'').to_string()
        } else {
            return Err(ForgeError::Parse("PREPARE: missing AS or FROM".into()));
        };

        let mut stmts = self.prepared_stmts.lock().unwrap();
        stmts.insert(name.to_lowercase(), sql);

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    fn handle_execute_prepared(&self, name: &str, session_txn: &mut Option<TxnId>) -> Result<ExecuteResult> {
        let sql = {
            let stmts = self.prepared_stmts.lock().unwrap();
            match stmts.get(&name.to_lowercase()) {
                Some(sql) => sql.clone(),
                None => return Err(ForgeError::Execution(format!("prepared statement '{}' not found", name))),
            }
        };
        self.execute_sql_session(&sql, session_txn)
    }

    // -----------------------------------------------------------------------
    // Backup / Restore
    // -----------------------------------------------------------------------

    fn handle_backup(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // BACKUP DATABASE TO 'path'
        // BACKUP DATABASE TO DISK = 'path'
        let path = if let Some(to_pos) = upper.find(" TO ") {
            let after_to = &text[to_pos + 4..].trim();
            let after_to = if after_to.to_uppercase().starts_with("DISK") {
                let eq_pos = after_to.find('=').unwrap_or(4);
                after_to[eq_pos + 1..].trim()
            } else {
                after_to
            };
            after_to.trim_matches('\'').trim_matches('"').to_string()
        } else {
            return Err(ForgeError::Parse("BACKUP: missing TO clause".into()));
        };

        // Flush all pages to disk
        self.cbpm.flush_all()?;
        let catalog = self.catalog.read().unwrap();
        catalog.persist()?;
        drop(catalog);

        // Copy database directory to backup path
        let src = &self.db_path;
        let dest = PathBuf::from(&path);
        std::fs::create_dir_all(&dest).map_err(|e| ForgeError::Io(e))?;

        // Copy all files from src to dest
        if let Ok(entries) = std::fs::read_dir(src) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let dest_file = dest.join(&file_name);
                let _ = std::fs::copy(entry.path(), dest_file);
            }
        }

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0,
            message: format!("Database backed up to {}", path),
        })
    }

    fn handle_restore(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // RESTORE DATABASE FROM 'path'
        // RESTORE DATABASE FROM DISK = 'path'
        let path = if let Some(from_pos) = upper.find(" FROM ") {
            let after_from = &text[from_pos + 6..].trim();
            let after_from = if after_from.to_uppercase().starts_with("DISK") {
                let eq_pos = after_from.find('=').unwrap_or(4);
                after_from[eq_pos + 1..].trim()
            } else {
                after_from
            };
            after_from.trim_matches('\'').trim_matches('"').to_string()
        } else {
            return Err(ForgeError::Parse("RESTORE: missing FROM clause".into()));
        };

        let src = PathBuf::from(&path);
        let dest = &self.db_path;

        // Copy all files from backup to database directory
        if let Ok(entries) = std::fs::read_dir(&src) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let dest_file = dest.join(&file_name);
                let _ = std::fs::copy(entry.path(), dest_file);
            }
        }

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0,
            message: format!("Database restored from {}", path),
        })
    }

    // -----------------------------------------------------------------------
    // Cursors
    // -----------------------------------------------------------------------

    fn handle_declare_cursor(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // DECLARE cursor_name CURSOR FOR select_statement
        let after_declare = &text[8..]; // skip "DECLARE "
        let name = after_declare.split_whitespace().next().unwrap_or("").to_string();

        let query_sql = if let Some(for_pos) = upper.find(" FOR ") {
            text[for_pos + 5..].trim().to_string()
        } else if let Some(cursor_pos) = upper.find("CURSOR") {
            // DECLARE name CURSOR FOR ...
            let after_cursor = &text[cursor_pos + 6..].trim();
            if after_cursor.to_uppercase().starts_with("FOR ") {
                after_cursor[4..].trim().to_string()
            } else {
                after_cursor.to_string()
            }
        } else {
            return Err(ForgeError::Parse("DECLARE CURSOR: missing FOR clause".into()));
        };

        // Store cursor definition (not yet opened)
        let mut cursors = self.cursors.lock().unwrap();
        cursors.insert(name.to_lowercase(), (vec![], vec![query_sql], 0));

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    fn handle_open_cursor(&self, name: &str, session_txn: &mut Option<TxnId>) -> Result<ExecuteResult> {
        let query_sql = {
            let cursors = self.cursors.lock().unwrap();
            match cursors.get(&name.to_lowercase()) {
                Some((_, cols, _)) if !cols.is_empty() && cols[0].to_uppercase().starts_with("SELECT") => {
                    cols[0].clone()
                },
                Some((_, cols, _)) if !cols.is_empty() => cols[0].clone(),
                _ => return Err(ForgeError::Execution(format!("cursor '{}' not found or not declared", name))),
            }
        };

        // Execute the query
        let result = self.execute_sql_session(&query_sql, session_txn)?;

        // Store the result set
        let mut cursors = self.cursors.lock().unwrap();
        cursors.insert(name.to_lowercase(), (result.rows, result.columns, 0));

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    fn handle_fetch_cursor(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // FETCH NEXT FROM cursor_name
        // FETCH FROM cursor_name
        let name = if let Some(from_pos) = upper.find(" FROM ") {
            text[from_pos + 6..].trim().to_string()
        } else {
            // FETCH cursor_name
            text[6..].trim().split_whitespace().last().unwrap_or("").to_string()
        };

        let mut cursors = self.cursors.lock().unwrap();
        match cursors.get_mut(&name.to_lowercase()) {
            Some((rows, columns, pos)) => {
                if *pos < rows.len() {
                    let row = rows[*pos].clone();
                    *pos += 1;
                    Ok(ExecuteResult {
                        columns: columns.clone(),
                        rows: vec![row],
                        rows_affected: 0, last_insert_id: 0, message: String::new(),
                    })
                } else {
                    // No more rows
                    Ok(ExecuteResult {
                        columns: columns.clone(),
                        rows: vec![],
                        rows_affected: 0, last_insert_id: 0, message: "DONE".into(),
                    })
                }
            }
            None => Err(ForgeError::Execution(format!("cursor '{}' not found", name))),
        }
    }

    // -----------------------------------------------------------------------
    // Plan cache management
    // -----------------------------------------------------------------------

    /// Invalidate plan cache (called after DDL changes)
    fn invalidate_plan_cache(&self) {
        let mut cache = self.plan_cache.write().unwrap();
        cache.clear();
        // Also clear the parse cache so schema changes are reflected
        crate::sql::parser::clear_parse_cache();
    }

    /// Extract the target table name from a DML plan node.
    fn extract_table_from_plan(plan: &PlanNode) -> Option<String> {
        match plan {
            PlanNode::Insert { table_name, .. } => Some(table_name.clone()),
            PlanNode::Update { table_name, .. } => Some(table_name.clone()),
            PlanNode::Delete { table_name, .. } => Some(table_name.clone()),
            _ => None,
        }
    }

    /// Check if a user has a specific privilege
    #[allow(dead_code)]
    pub fn check_privilege(&self, username: &str, privilege: &str, table: Option<&str>) -> bool {
        let users = self.users.lock().unwrap();
        if let Some((_, privileges)) = users.get(&username.to_lowercase()) {
            let key = format!("{}:{}", privilege.to_uppercase(), table.unwrap_or("*"));
            let all_key = format!("ALL:{}", table.unwrap_or("*"));
            let global_key = format!("{}:*", privilege.to_uppercase());
            let global_all = "ALL:*".to_string();
            privileges.contains(&key) || privileges.contains(&all_key)
                || privileges.contains(&global_key) || privileges.contains(&global_all)
        } else {
            false
        }
    }

    /// Get the current max memory per query setting
    #[allow(dead_code)]
    pub fn get_max_memory_per_query(&self) -> usize {
        *self.max_memory_per_query.lock().unwrap()
    }

    /// Clean up temp tables created by a connection (# prefix)
    pub fn cleanup_temp_tables(&self, conn_id: u32) {
        let prefix = format!("#tmp_{}__", conn_id);
        let catalog = self.catalog.read().unwrap();
        let tables: Vec<String> = catalog.list_tables().iter()
            .filter(|t| t.name.starts_with(&prefix))
            .map(|t| t.name.clone())
            .collect();
        drop(catalog);

        for table_name in tables {
            let _ = self.execute_sql(&format!("DROP TABLE IF EXISTS {}", table_name));
        }
    }

    /// Resolve a temp table name with connection prefix
    pub fn resolve_temp_table_name(table_name: &str, conn_id: u32) -> String {
        if table_name.starts_with('#') && !table_name.starts_with("##") {
            format!("#tmp_{}_{}", conn_id, &table_name[1..])
        } else {
            table_name.to_string()
        }
    }

    /// Shut down the database cleanly.
    /// Read-only catalog reference for direct access (used by ForgeWire columnar join).
    pub fn catalog_ref(&self) -> std::sync::RwLockReadGuard<'_, Catalog> {
        self.catalog.read().unwrap()
    }

    /// Read-only indexes reference.
    pub fn indexes_ref(&self) -> std::sync::RwLockReadGuard<'_, Vec<(String, crate::index::BTreeIndex)>> {
        self.indexes.read().unwrap()
    }

    /// ConcurrentBufferPool reference for direct page access.
    pub fn cbpm_ref(&self) -> &ConcurrentBufferPool {
        &self.cbpm
    }

    pub fn shutdown(&self) -> Result<()> {
        let catalog = self.catalog.read().unwrap();
        catalog.persist()?;
        self.cbpm.flush_all()?;
        Ok(())
    }

    /// Get resource counts for leak detection. Returns (savepoints, sequences,
    /// plan_cache, cursors, procedures, triggers_total, users, prepared_stmts, partitions).
    pub fn resource_counts(&self) -> (usize, usize, usize, usize, usize, usize, usize, usize, usize) {
        let sp = self.savepoints.lock().unwrap().len();
        let seq = self.sequences.lock().unwrap().len();
        let pc = self.plan_cache.read().unwrap().len();
        let cur = self.cursors.lock().unwrap().len();
        let proc = self.procedures.lock().unwrap().len();
        let trg: usize = self.triggers.lock().unwrap().values().map(|v| v.len()).sum();
        let usr = self.users.lock().unwrap().len();
        let ps = self.prepared_stmts.lock().unwrap().len();
        let pt = self.partitions.read().unwrap().len();
        (sp, seq, pc, cur, proc, trg, usr, ps, pt)
    }

    // -----------------------------------------------------------------------
    // Partitioned Tables
    // -----------------------------------------------------------------------

    /// Handle CREATE TABLE ... PARTITION BY RANGE(col) (PARTITION p1 VALUES LESS THAN (100), ...)
    fn handle_create_partitioned_table(&self, sql_text: &str) -> Result<ExecuteResult> {
        let text = sql_text.trim().trim_end_matches(';').trim();
        let upper = text.to_uppercase();

        // Extract table name: CREATE TABLE [IF NOT EXISTS] table_name (
        let after_create = upper.trim_start_matches("CREATE TABLE").trim();
        let after_create_orig = text.trim_start_matches("CREATE TABLE").trim_start_matches("create table").trim();
        let (table_name, rest) = if after_create.starts_with("IF NOT EXISTS") {
            let s = after_create.trim_start_matches("IF NOT EXISTS").trim();
            let orig = after_create_orig
                .trim_start_matches("IF NOT EXISTS").trim_start_matches("if not exists").trim();
            let end = s.find('(').unwrap_or(s.len());
            let end_orig = orig.find('(').unwrap_or(orig.len());
            (s[..end].trim().to_string(), orig[end_orig..].to_string())
        } else {
            let end = after_create.find('(').unwrap_or(after_create.len());
            let end_orig = after_create_orig.find('(').unwrap_or(after_create_orig.len());
            (after_create[..end].trim().to_string(), after_create_orig[end_orig..].to_string())
        };

        let table_name_lower = table_name.to_lowercase().replace('`', "").replace('"', "");

        // Find PARTITION BY RANGE
        let rest_upper = rest.to_uppercase();
        let part_pos = rest_upper.find("PARTITION BY RANGE").ok_or_else(|| {
            ForgeError::Parse("missing PARTITION BY RANGE clause".into())
        })?;

        // Extract column definitions (before PARTITION BY)
        let columns_part = &rest[..part_pos];
        // Remove trailing ) if present, and leading (
        let columns_sql = columns_part.trim().trim_start_matches('(').trim();
        // Find the last closing paren that matches the opening one for columns
        let columns_sql = columns_sql.trim_end_matches(')').trim().trim_end_matches(',').trim();

        // Extract partition column
        let after_partition = &rest[part_pos..];
        let after_partition_upper = after_partition.to_uppercase();
        let range_start = after_partition_upper.find("RANGE").ok_or_else(|| {
            ForgeError::Parse("missing RANGE keyword in partition definition".into())
        })? + 5;
        let paren_start = after_partition[range_start..].find('(').ok_or_else(|| {
            ForgeError::Parse("missing partition column".into())
        })? + range_start;
        let paren_end = after_partition[paren_start..].find(')').ok_or_else(|| {
            ForgeError::Parse("missing closing paren for partition column".into())
        })? + paren_start;
        let partition_col = after_partition[paren_start + 1..paren_end].trim().to_lowercase()
            .replace('`', "").replace('"', "");

        // Parse partition definitions: PARTITION p1 VALUES LESS THAN (100), ...
        let after_col_paren = &after_partition[paren_end + 1..];
        let after_col_upper = after_col_paren.to_uppercase();

        // Find the opening paren of the partition list
        let list_start = after_col_upper.find('(').ok_or_else(|| {
            ForgeError::Parse("missing partition list".into())
        })?;
        let partition_list_str = &after_col_paren[list_start + 1..];

        // Parse each PARTITION entry
        let mut range_partitions: Vec<(String, i64)> = Vec::new();
        let entries_upper = partition_list_str.to_uppercase();
        let mut pos = 0;
        while let Some(p) = entries_upper[pos..].find("PARTITION ") {
            pos += p + 10; // skip "PARTITION "
            let name_end = entries_upper[pos..].find(char::is_whitespace).unwrap_or(entries_upper.len() - pos);
            let part_name = partition_list_str[pos..pos + name_end].trim().to_lowercase()
                .replace('`', "").replace('"', "");
            pos += name_end;

            // Find VALUES LESS THAN
            if let Some(vlt) = entries_upper[pos..].find("VALUES LESS THAN") {
                pos += vlt + 16; // skip "VALUES LESS THAN"
                let val_start = entries_upper[pos..].find('(');
                if let Some(vs) = val_start {
                    pos += vs + 1;
                    let val_end = entries_upper[pos..].find(')').unwrap_or(entries_upper.len() - pos);
                    let val_str = partition_list_str[pos..pos + val_end].trim();
                    let bound = if val_str.eq_ignore_ascii_case("MAXVALUE") {
                        i64::MAX
                    } else {
                        val_str.parse::<i64>().unwrap_or(i64::MAX)
                    };
                    range_partitions.push((part_name, bound));
                    pos += val_end + 1;
                } else {
                    // VALUES LESS THAN MAXVALUE (without parens)
                    let rest_trimmed = entries_upper[pos..].trim();
                    if rest_trimmed.starts_with("MAXVALUE") || rest_trimmed.starts_with(" MAXVALUE") {
                        range_partitions.push((part_name, i64::MAX));
                        pos += 8;
                    }
                }
            }
        }

        if range_partitions.is_empty() {
            return Err(ForgeError::Parse("no partitions defined".into()));
        }

        // Create sub-tables for each partition
        for (part_name, _) in &range_partitions {
            let sub_table = format!("{}__p_{}", table_name_lower, part_name);
            let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", sub_table, columns_sql);
            self.execute_sql(&create_sql)?;
        }

        // Store partition metadata
        let meta = PartitionMeta {
            column: partition_col,
            range_partitions,
            columns_sql: columns_sql.to_string(),
        };
        let mut parts = self.partitions.write().unwrap();
        parts.insert(table_name_lower, meta);
        self.has_partitions.store(true, std::sync::atomic::Ordering::Relaxed);

        Ok(ExecuteResult {
            rows: vec![], columns: vec![],
            rows_affected: 0, last_insert_id: 0, message: "OK".into(),
        })
    }

    /// Resolve INSERT/SELECT on partitioned tables to actual sub-tables.
    fn resolve_partitioned_sql(&self, sql_text: &str, upper: &str) -> Result<String> {
        let parts = self.partitions.read().unwrap();
        if parts.is_empty() {
            return Ok(sql_text.to_string());
        }

        // Handle INSERT INTO partitioned_table
        if upper.starts_with("INSERT ") {
            // Extract table name from INSERT INTO table_name
            let after_into = if let Some(pos) = upper.find("INTO ") {
                &sql_text[pos + 5..]
            } else {
                return Ok(sql_text.to_string());
            };
            let table_end = after_into.find(|c: char| c == '(' || c.is_whitespace())
                .unwrap_or(after_into.len());
            let table_name = after_into[..table_end].trim().to_lowercase()
                .replace('`', "").replace('"', "");

            if let Some(meta) = parts.get(&table_name) {
                // Clone metadata so we can drop the lock
                let meta = meta.clone();
                drop(parts);
                return self.route_insert_to_partition(sql_text, &table_name, &meta);
            }
        }

        // Handle SELECT FROM partitioned_table
        if upper.starts_with("SELECT ") || upper.contains(" FROM ") {
            // Check if any FROM clause references a partitioned table
            for (base_table, meta) in parts.iter() {
                // Simple check: does the FROM clause contain this table name?
                let patterns = [
                    format!("from {}", base_table),
                    format!("from `{}`", base_table),
                    format!("from \"{}\"", base_table),
                ];
                let lower_sql = sql_text.to_lowercase();
                for pattern in &patterns {
                    if lower_sql.contains(pattern) {
                        // Rewrite: replace the table reference with a UNION ALL subquery
                        let union_parts: Vec<String> = meta.range_partitions.iter()
                            .map(|(pname, _)| format!("SELECT * FROM {}__p_{}", base_table, pname))
                            .collect();
                        let union_query = format!("({})", union_parts.join(" UNION ALL "));

                        // Find the exact position and replace
                        // We need to handle aliases too
                        let from_pos = match lower_sql.find(pattern) {
                            Some(pos) => pos,
                            None => return Ok(sql_text.to_string()),
                        };
                        let table_start = from_pos + 5; // "from " = 5 chars
                        let table_end = table_start + base_table.len();
                        // Check for alias after table name
                        let rest_after = &sql_text[table_end..];
                        let rest_trimmed = rest_after.trim_start();

                        let mut new_sql = String::new();
                        new_sql.push_str(&sql_text[..table_start]);
                        new_sql.push_str(&union_query);
                        // Add alias for the subquery
                        if !rest_trimmed.is_empty() && !rest_trimmed.starts_with("WHERE")
                            && !rest_trimmed.starts_with("where")
                            && !rest_trimmed.starts_with("ORDER")
                            && !rest_trimmed.starts_with("order")
                            && !rest_trimmed.starts_with("GROUP")
                            && !rest_trimmed.starts_with("group")
                            && !rest_trimmed.starts_with("LIMIT")
                            && !rest_trimmed.starts_with("limit")
                            && !rest_trimmed.starts_with("HAVING")
                            && !rest_trimmed.starts_with("having")
                            && !rest_trimmed.starts_with(';')
                        {
                            new_sql.push_str(&sql_text[table_end..]);
                        } else {
                            new_sql.push_str(&format!(" AS {}", base_table));
                            new_sql.push_str(&sql_text[table_end..]);
                        }

                        return Ok(new_sql);
                    }
                }
            }
        }

        Ok(sql_text.to_string())
    }

    /// Route an INSERT statement to the correct partition sub-table.
    fn route_insert_to_partition(&self, sql_text: &str, base_table: &str, meta: &PartitionMeta) -> Result<String> {
        // Find column list if specified
        let lower = sql_text.to_lowercase();
        let into_pos = lower.find(&format!("into {}", base_table))
            .or_else(|| lower.find(&format!("into `{}`", base_table)))
            .unwrap_or(0);
        let after_table = &sql_text[into_pos + 5 + base_table.len()..];
        let after_table = after_table.trim_start_matches('`').trim_start_matches('"');

        // Check if there's a column list
        let trimmed = after_table.trim();
        let (col_names, values_start) = if trimmed.starts_with('(') && !trimmed.to_uppercase().starts_with("(SELECT") {
            // Find the closing paren for columns
            let close = trimmed.find(')').unwrap_or(0);
            let col_str = &trimmed[1..close];
            let cols: Vec<String> = col_str.split(',')
                .map(|s| s.trim().to_lowercase().replace('`', "").replace('"', ""))
                .collect();
            (Some(cols), &trimmed[close + 1..])
        } else {
            (None, trimmed)
        };

        // Find partition column index
        let part_col_idx = if let Some(ref cols) = col_names {
            cols.iter().position(|c| c == &meta.column)
        } else {
            // If no column list, use catalog to find the column index
            let catalog = self.catalog.read().unwrap();
            // Try to find the sub-table to get column order
            let first_sub = format!("{}__p_{}", base_table, meta.range_partitions[0].0);
            if let Some(info) = catalog.get_table(&first_sub) {
                info.schema.columns.iter().position(|c| c.name.to_lowercase() == meta.column)
            } else {
                None
            }
        };

        let part_col_idx = match part_col_idx {
            Some(idx) => idx,
            None => {
                // Can't determine partition column, insert into first partition
                let target = format!("{}__p_{}", base_table, meta.range_partitions[0].0);
                return Ok(sql_text.replace(base_table, &target));
            }
        };

        // Find VALUES clause
        let values_upper = values_start.to_uppercase();
        if let Some(vpos) = values_upper.find("VALUES") {
            let after_values = &values_start[vpos + 6..].trim();
            // Parse first value tuple to find the partition column value
            if let Some(open_paren) = after_values.find('(') {
                let inner = &after_values[open_paren + 1..];
                let close_paren = inner.find(')').unwrap_or(inner.len());
                let values_str = &inner[..close_paren];
                let vals: Vec<&str> = values_str.split(',').collect();
                if part_col_idx < vals.len() {
                    let val_str = vals[part_col_idx].trim().trim_matches('\'').trim_matches('"');
                    let val: i64 = val_str.parse().unwrap_or(0);

                    // Find the correct partition
                    for (pname, upper_bound) in &meta.range_partitions {
                        if val < *upper_bound {
                            let target = format!("{}__p_{}", base_table, pname);
                            let orig_lower = sql_text.to_lowercase();
                            let pos = orig_lower.find(base_table).unwrap_or(0);
                            let mut new_sql = String::new();
                            new_sql.push_str(&sql_text[..pos]);
                            new_sql.push_str(&target);
                            new_sql.push_str(&sql_text[pos + base_table.len()..]);
                            return Ok(new_sql);
                        }
                    }
                    // Value exceeds all bounds — insert into last partition
                    let last = &meta.range_partitions.last().ok_or_else(|| {
                        ForgeError::Execution("partitioned table has no partitions".into())
                    })?.0;
                    let target = format!("{}__p_{}", base_table, last);
                    let orig_lower = sql_text.to_lowercase();
                    let pos = orig_lower.find(base_table).unwrap_or(0);
                    let mut new_sql = String::new();
                    new_sql.push_str(&sql_text[..pos]);
                    new_sql.push_str(&target);
                    new_sql.push_str(&sql_text[pos + base_table.len()..]);
                    return Ok(new_sql);
                }
            }
        }

        // Fallback: route to first partition
        let target = format!("{}__p_{}", base_table, meta.range_partitions[0].0);
        let orig_lower = sql_text.to_lowercase();
        if let Some(pos) = orig_lower.find(base_table) {
            let mut new_sql = String::new();
            new_sql.push_str(&sql_text[..pos]);
            new_sql.push_str(&target);
            new_sql.push_str(&sql_text[pos + base_table.len()..]);
            Ok(new_sql)
        } else {
            Ok(sql_text.to_string())
        }
    }
}

/// Convert a runtime Value to a literal Expr for inlining subquery results.
fn value_to_literal_expr(val: &Value) -> Expr {
    match val {
        Value::Integer(n) => Expr::Literal(LiteralValue::Integer(*n as i64)),
        Value::BigInt(n) => Expr::Literal(LiteralValue::Integer(*n)),
        Value::Float(f) => Expr::Literal(LiteralValue::Float(*f)),
        Value::Varchar(s) => Expr::Literal(LiteralValue::String(s.clone())),
        Value::Boolean(b) => Expr::Literal(LiteralValue::Boolean(*b)),
        Value::DateTime(e) => Expr::Literal(LiteralValue::Integer(*e)),
        Value::Decimal(v, scale) => {
            let f = *v as f64 / 10f64.powi(*scale as i32);
            Expr::Literal(LiteralValue::Float(f))
        }
        Value::Date(d) => Expr::Literal(LiteralValue::Integer(*d as i64)),
        Value::Time(t) => Expr::Literal(LiteralValue::Integer(*t as i64)),
        Value::Binary(_) => Expr::Literal(LiteralValue::Null),
        Value::Json(s) => Expr::Literal(LiteralValue::String(s.clone())),
        Value::Uuid(s) => Expr::Literal(LiteralValue::String(s.clone())),
        Value::Null => Expr::Literal(LiteralValue::Null),
    }
}
