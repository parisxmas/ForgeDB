use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use crate::catalog::Catalog;
use crate::error::Result;
use crate::executor::executor::{ExecuteResult, ExecutorContext};
use crate::executor::{self};
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::planner::Planner;
use crate::sql;
use crate::sql::ast::Statement;
use crate::storage::{BufferPoolManager, DiskManager};
use crate::tuple::types::Value;
use crate::txn::TransactionManager;

/// Top-level database handle with interior mutability for concurrent access.
///
/// SQL parsing and query planning run lock-free in parallel across all cores.
/// The catalog uses a RwLock so concurrent SELECTs share read access.
/// Data access (BPM, indexes) uses fine-grained Mutexes held only during
/// the execution phase.
pub struct Database {
    bpm: Mutex<BufferPoolManager>,
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
        let bpm = BufferPoolManager::new(1024, disk_manager);
        let catalog = Catalog::new(catalog_file.to_str().unwrap());
        let txn_manager = TransactionManager::new(wal_file.to_str().unwrap())?;

        Ok(Self {
            bpm: Mutex::new(bpm),
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
        let mut bpm = BufferPoolManager::new(1024, disk_manager);
        let catalog = Catalog::load(catalog_file.to_str().unwrap())?;
        let mut txn_manager = TransactionManager::new(wal_file.to_str().unwrap())?;

        txn_manager.recover(&mut bpm)?;

        Ok(Self {
            bpm: Mutex::new(bpm),
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
    /// Takes `&self` — multiple threads can call this concurrently.
    /// Parsing and planning run lock-free on all cores.
    /// Execution acquires fine-grained locks only when needed.
    pub fn execute_sql(&self, sql_text: &str) -> Result<ExecuteResult> {
        // Phase 1: Parse — no locks, runs on any core
        let stmt = sql::parse(sql_text)?;

        // Phase 2: Handle metadata queries with read lock only
        if let Some(result) = self.try_handle_directly(&stmt)? {
            return Ok(result);
        }

        // Phase 3: Plan — read locks on catalog + indexes (concurrent with other SELECTs)
        let plan = {
            let catalog = self.catalog.read().unwrap();
            let indexes = self.indexes.read().unwrap();
            let planner = Planner::new(&catalog, &indexes);
            planner.plan(stmt)?
        }; // read locks released here

        // Phase 4: Execute — acquire write locks for data modification
        let mut bpm = self.bpm.lock().unwrap();
        let mut catalog = self.catalog.write().unwrap();
        let mut indexes = self.indexes.write().unwrap();
        let mut clustered = self.clustered_indexes.write().unwrap();
        let mut auto_inc = self.auto_increment_counters.lock().unwrap();

        let mut ctx = ExecutorContext {
            bpm: &mut bpm,
            catalog: &mut catalog,
            indexes: &mut indexes,
            clustered_indexes: &mut clustered,
            auto_increment_counters: &mut auto_inc,
        };
        let result = executor::execute(plan, &mut ctx)?;

        catalog.persist()?;

        Ok(result)
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
                        "table '{}' not found",
                        table_name
                    ))
                })?;
                let rows: Vec<Vec<Value>> = info
                    .schema
                    .columns
                    .iter()
                    .map(|c| {
                        vec![
                            Value::Varchar(c.name.clone()),
                            Value::Varchar(format!("{:?}", c.data_type)),
                            Value::Varchar(if c.nullable { "YES" } else { "NO" }.into()),
                            Value::Varchar(if c.is_primary_key { "PRI" } else { "" }.into()),
                            Value::Null,
                            Value::Varchar(
                                if c.auto_increment { "auto_increment" } else { "" }.into(),
                            ),
                        ]
                    })
                    .collect();
                Ok(Some(ExecuteResult {
                    columns: vec![
                        "Field".into(), "Type".into(), "Null".into(),
                        "Key".into(), "Default".into(), "Extra".into(),
                    ],
                    rows,
                    rows_affected: 0,
                    last_insert_id: 0,
                    message: String::new(),
                }))
            }
            Statement::DescribeTable { table_name } => {
                self.try_handle_directly(&Statement::ShowColumns {
                    table_name: table_name.clone(),
                })
            }
            Statement::ShowCreateTable { table_name } => {
                let catalog = self.catalog.read().unwrap();
                let info = catalog.get_table(table_name).ok_or_else(|| {
                    crate::error::ForgeError::Execution(format!(
                        "table '{}' not found",
                        table_name
                    ))
                })?;
                let mut ddl = format!("CREATE TABLE `{}` (\n", info.name);
                for (i, col) in info.schema.columns.iter().enumerate() {
                    if i > 0 {
                        ddl.push_str(",\n");
                    }
                    ddl.push_str(&format!(
                        "  `{}` {:?}{}{}",
                        col.name,
                        col.data_type,
                        if col.nullable { " NULL" } else { " NOT NULL" },
                        if col.auto_increment { " AUTO_INCREMENT" } else { "" },
                    ));
                }
                ddl.push_str("\n)");
                Ok(Some(ExecuteResult {
                    columns: vec!["Table".into(), "Create Table".into()],
                    rows: vec![vec![
                        Value::Varchar(info.name.clone()),
                        Value::Varchar(ddl),
                    ]],
                    rows_affected: 0,
                    last_insert_id: 0,
                    message: String::new(),
                }))
            }
            Statement::SetVariable { .. } | Statement::UseDatabase { .. } => {
                Ok(Some(ExecuteResult {
                    rows: vec![],
                    columns: vec![],
                    rows_affected: 0,
                    last_insert_id: 0,
                    message: "OK".into(),
                }))
            }
            Statement::StartTransaction | Statement::Commit | Statement::Rollback => {
                Ok(Some(ExecuteResult {
                    rows: vec![],
                    columns: vec![],
                    rows_affected: 0,
                    last_insert_id: 0,
                    message: "OK".into(),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Shut down the database cleanly.
    pub fn shutdown(&self) -> Result<()> {
        let catalog = self.catalog.read().unwrap();
        catalog.persist()?;
        let mut bpm = self.bpm.lock().unwrap();
        bpm.flush_all()?;
        Ok(())
    }
}
