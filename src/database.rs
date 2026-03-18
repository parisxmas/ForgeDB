use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use crate::catalog::Catalog;
use crate::error::Result;
use crate::executor::executor::{ExecuteResult, ExecutorContext};
use crate::executor::{self};
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::planner::plan::PlanNode;
use crate::planner::Planner;
use crate::sql;
use crate::sql::ast::Statement;
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::DiskManager;
use crate::tuple::types::Value;
use crate::txn::TransactionManager;

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
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl Database {
    /// Create a new database at the given directory path.
    pub fn new(path: &str) -> Result<Self> {
        let db_path = PathBuf::from(path);
        std::fs::create_dir_all(&db_path)?;

        let data_file = db_path.join("forgedb.data");
        let catalog_file = db_path.join("forgedb.catalog");
        let wal_file = db_path.join("forgedb.wal");

        let disk_manager = DiskManager::new(data_file.to_str().unwrap())?;
        let cbpm = ConcurrentBufferPool::new(1024, disk_manager);
        let catalog = Catalog::new(catalog_file.to_str().unwrap());
        let txn_manager = TransactionManager::new(wal_file.to_str().unwrap())?;

        Ok(Self {
            cbpm,
            catalog: RwLock::new(catalog),
            txn_manager: Mutex::new(txn_manager),
            indexes: RwLock::new(Vec::new()),
            clustered_indexes: RwLock::new(HashMap::new()),
            auto_increment_counters: Mutex::new(HashMap::new()),
            db_path,
        })
    }

    /// Open an existing database at the given directory path.
    pub fn open(path: &str) -> Result<Self> {
        let db_path = PathBuf::from(path);

        let data_file = db_path.join("forgedb.data");
        let catalog_file = db_path.join("forgedb.catalog");
        let wal_file = db_path.join("forgedb.wal");

        let disk_manager = DiskManager::new(data_file.to_str().unwrap())?;
        let cbpm = ConcurrentBufferPool::new(1024, disk_manager);
        let catalog = Catalog::load(catalog_file.to_str().unwrap())?;
        let mut txn_manager = TransactionManager::new(wal_file.to_str().unwrap())?;

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
            db_path,
        })
    }

    /// Parse, plan, and execute a SQL statement.
    ///
    /// Takes `&self` — multiple threads call this concurrently.
    /// Each thread gets its own LocalBpm handle with page-level locks,
    /// so threads operating on different pages run truly in parallel.
    pub fn execute_sql(&self, sql_text: &str) -> Result<ExecuteResult> {
        // Phase 1: Parse — no locks, fully parallel across all cores
        let stmt = sql::parse(sql_text)?;

        // Phase 2: Handle metadata queries with read lock
        if let Some(result) = self.try_handle_directly(&stmt)? {
            return Ok(result);
        }

        // Phase 3: Plan — read locks on catalog + indexes (concurrent readers)
        let plan = {
            let catalog = self.catalog.read().unwrap();
            let indexes = self.indexes.read().unwrap();
            let planner = Planner::new(&catalog, &indexes);
            planner.plan(stmt)?
        }; // read locks released

        // Phase 4: Execute — detect read-only queries for maximum parallelism
        let is_read_only = matches!(
            plan,
            PlanNode::SeqScan { .. }
            | PlanNode::IndexScan { .. }
            | PlanNode::Filter { .. }
            | PlanNode::Projection { .. }
            | PlanNode::NestedLoopJoin { .. }
            | PlanNode::Sort { .. }
            | PlanNode::Limit { .. }
        );

        if is_read_only {
            // READ PATH: each thread gets its own snapshot — zero contention
            // Clone is cheap: catalog is just HashMap<String, TableInfo> metadata,
            // indexes/clustered are Vec/HashMap of small structs (no page data).
            let mut snap_catalog = {
                let guard = self.catalog.read().unwrap();
                guard.clone()
            };
            let mut snap_indexes = {
                let guard = self.indexes.read().unwrap();
                guard.clone()
            };
            let mut snap_clustered = {
                let guard = self.clustered_indexes.read().unwrap();
                guard.clone()
            };
            let mut empty_auto_inc = HashMap::new();

            let mut local_bpm = LocalBpm::new(&self.cbpm);
            let result = {
                let mut ctx = ExecutorContext {
                    bpm: &mut local_bpm,
                    catalog: &mut snap_catalog,
                    indexes: &mut snap_indexes,
                    clustered_indexes: &mut snap_clustered,
                    auto_increment_counters: &mut empty_auto_inc,
                };
                executor::execute(plan, &mut ctx)?
            };
            Ok(result)
        } else {
            // WRITE PATH: exclusive locks for DML/DDL
            let mut catalog = self.catalog.write().unwrap();
            let mut indexes = self.indexes.write().unwrap();
            let mut clustered = self.clustered_indexes.write().unwrap();
            let mut auto_inc = self.auto_increment_counters.lock().unwrap();

            let mut local_bpm = LocalBpm::new(&self.cbpm);
            let result = {
                let mut ctx = ExecutorContext {
                    bpm: &mut local_bpm,
                    catalog: &mut catalog,
                    indexes: &mut indexes,
                    clustered_indexes: &mut clustered,
                    auto_increment_counters: &mut auto_inc,
                };
                executor::execute(plan, &mut ctx)?
            };
            drop(local_bpm);

            catalog.persist()?;

            Ok(result)
        }
    }

    /// Handle metadata/session statements with minimal locking.
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
                    rows_affected: 0,
                    last_insert_id: 0,
                    message: String::new(),
                }))
            }
            Statement::ShowColumns { table_name } => {
                let catalog = self.catalog.read().unwrap();
                let info = catalog.get_table(table_name).ok_or_else(|| {
                    crate::error::ForgeError::Execution(format!(
                        "table '{}' not found", table_name
                    ))
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
                    rows_affected: 0, last_insert_id: 0,
                    message: String::new(),
                }))
            }
            Statement::DescribeTable { table_name } => {
                self.try_handle_directly(&Statement::ShowColumns { table_name: table_name.clone() })
            }
            Statement::ShowCreateTable { table_name } => {
                let catalog = self.catalog.read().unwrap();
                let info = catalog.get_table(table_name).ok_or_else(|| {
                    crate::error::ForgeError::Execution(format!("table '{}' not found", table_name))
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
            Statement::SetVariable { .. } | Statement::UseDatabase { .. }
            | Statement::StartTransaction | Statement::Commit | Statement::Rollback => {
                Ok(Some(ExecuteResult {
                    rows: vec![], columns: vec![],
                    rows_affected: 0, last_insert_id: 0, message: "OK".into(),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Shut down the database cleanly.
    pub fn shutdown(&self) -> Result<()> {
        let catalog = self.catalog.read().unwrap();
        catalog.persist()?;
        self.cbpm.flush_all()?;
        Ok(())
    }
}
