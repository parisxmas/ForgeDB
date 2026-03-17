use std::path::PathBuf;

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

/// Top-level database handle.
pub struct Database {
    pub bpm: BufferPoolManager,
    pub catalog: Catalog,
    pub txn_manager: TransactionManager,
    pub indexes: Vec<(String, BTreeIndex)>,
    pub clustered_indexes: std::collections::HashMap<String, ClusteredIndex>,
    pub auto_increment_counters: std::collections::HashMap<String, i64>,
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
            bpm,
            catalog,
            txn_manager,
            indexes: Vec::new(),
            clustered_indexes: std::collections::HashMap::new(),
            auto_increment_counters: std::collections::HashMap::new(),
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
            bpm,
            catalog,
            txn_manager,
            indexes: Vec::new(),
            clustered_indexes: std::collections::HashMap::new(),
            auto_increment_counters: std::collections::HashMap::new(),
            db_path,
        })
    }

    /// Parse, plan, and execute a SQL statement.
    pub fn execute_sql(&mut self, sql_text: &str) -> Result<ExecuteResult> {
        let stmt = sql::parse(sql_text)?;

        // Handle statements that bypass planner
        if let Some(result) = self.try_handle_directly(&stmt)? {
            return Ok(result);
        }

        // Plan
        let plan = {
            let planner = Planner::new(&self.catalog, &self.indexes);
            planner.plan(stmt)?
        };

        // Execute
        let mut ctx = ExecutorContext {
            bpm: &mut self.bpm,
            catalog: &mut self.catalog,
            indexes: &mut self.indexes,
            clustered_indexes: &mut self.clustered_indexes,
            auto_increment_counters: &mut self.auto_increment_counters,
        };
        let result = executor::execute(plan, &mut ctx)?;

        self.catalog.persist()?;

        Ok(result)
    }

    /// Handle statements that don't need the planner.
    fn try_handle_directly(&mut self, stmt: &Statement) -> Result<Option<ExecuteResult>> {
        match stmt {
            Statement::ShowTables => {
                let tables = self.catalog.list_tables();
                let mut rows: Vec<Vec<Value>> = tables
                    .iter()
                    .map(|t| vec![Value::Varchar(t.name.clone())])
                    .collect();
                rows.sort_by(|a, b| a[0].compare(&b[0]).unwrap_or(std::cmp::Ordering::Equal));
                Ok(Some(ExecuteResult {
                    columns: vec!["Tables_in_forgedb".to_string()],
                    rows,
                    rows_affected: 0,
                    message: String::new(),
                }))
            }
            Statement::ShowColumns { table_name } => {
                let info = self.catalog.get_table(table_name).ok_or_else(|| {
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
                            Value::Varchar(
                                if c.is_primary_key { "PRI" } else { "" }.into(),
                            ),
                            Value::Null,
                            Value::Varchar(
                                if c.auto_increment {
                                    "auto_increment"
                                } else {
                                    ""
                                }
                                .into(),
                            ),
                        ]
                    })
                    .collect();
                Ok(Some(ExecuteResult {
                    columns: vec![
                        "Field".into(),
                        "Type".into(),
                        "Null".into(),
                        "Key".into(),
                        "Default".into(),
                        "Extra".into(),
                    ],
                    rows,
                    rows_affected: 0,
                    message: String::new(),
                }))
            }
            Statement::DescribeTable { table_name } => {
                // Same as SHOW COLUMNS
                self.try_handle_directly(&Statement::ShowColumns {
                    table_name: table_name.clone(),
                })
            }
            Statement::ShowCreateTable { table_name } => {
                let info = self.catalog.get_table(table_name).ok_or_else(|| {
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
                        if col.auto_increment {
                            " AUTO_INCREMENT"
                        } else {
                            ""
                        },
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
                    message: String::new(),
                }))
            }
            Statement::SetVariable { .. } | Statement::UseDatabase { .. } => {
                Ok(Some(ExecuteResult {
                    rows: vec![],
                    columns: vec![],
                    rows_affected: 0,
                    message: "OK".into(),
                }))
            }
            Statement::StartTransaction | Statement::Commit | Statement::Rollback => {
                Ok(Some(ExecuteResult {
                    rows: vec![],
                    columns: vec![],
                    rows_affected: 0,
                    message: "OK".into(),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Shut down the database cleanly.
    pub fn shutdown(&mut self) -> Result<()> {
        self.catalog.persist()?;
        self.bpm.flush_all()?;
        Ok(())
    }
}
