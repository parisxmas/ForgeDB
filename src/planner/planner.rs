use crate::catalog::Catalog;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::planner::plan::PlanNode;
use crate::sql::ast::*;

pub struct Planner<'a> {
    catalog: &'a Catalog,
    indexes: &'a [(String, BTreeIndex)],
}

impl<'a> Planner<'a> {
    pub fn new(catalog: &'a Catalog, indexes: &'a [(String, BTreeIndex)]) -> Self {
        Self { catalog, indexes }
    }

    pub fn plan(&self, stmt: Statement) -> Result<PlanNode> {
        match stmt {
            Statement::CreateTable {
                table_name,
                columns,
                if_not_exists,
            } => {
                if if_not_exists && self.catalog.get_table(&table_name).is_some() {
                    return Ok(PlanNode::CreateTable {
                        table_name: String::new(), // signal: skip
                        columns: vec![],
                    });
                }
                Ok(PlanNode::CreateTable {
                    table_name,
                    columns,
                })
            }
            Statement::DropTable { table_name, if_exists } => {
                if if_exists && self.catalog.get_table(&table_name).is_none() {
                    return Ok(PlanNode::DropTable {
                        table_name: String::new(), // signal: skip
                    });
                }
                Ok(PlanNode::DropTable { table_name })
            }
            Statement::Insert {
                table_name,
                columns,
                values,
            } => Ok(PlanNode::Insert {
                table_name,
                columns,
                values,
            }),
            Statement::Select {
                columns,
                from,
                r#where,
                order_by,
                limit,
            } => self.plan_select(columns, from, r#where, order_by, limit),
            Statement::Update {
                table_name,
                assignments,
                r#where,
            } => self.plan_update(table_name, assignments, r#where),
            Statement::Delete {
                table_name,
                r#where,
            } => self.plan_delete(table_name, r#where),
            Statement::CreateIndex {
                index_name,
                table_name,
                columns,
                unique,
            } => Ok(PlanNode::CreateIndex {
                index_name,
                table_name,
                columns,
                unique,
            }),
            // Handled directly in Database::execute_sql
            Statement::ShowTables
            | Statement::ShowColumns { .. }
            | Statement::ShowCreateTable { .. }
            | Statement::DescribeTable { .. }
            | Statement::SetVariable { .. }
            | Statement::UseDatabase { .. }
            | Statement::StartTransaction
            | Statement::Commit
            | Statement::Rollback
            | Statement::AlterTable { .. } => {
                Err(ForgeError::Plan("statement type not yet supported in planner".into()))
            }
        }
    }

    fn plan_select(
        &self,
        columns: Vec<SelectColumn>,
        from: FromClause,
        where_clause: Option<Expr>,
        order_by: Vec<OrderByItem>,
        limit: Option<usize>,
    ) -> Result<PlanNode> {
        // Build scan from FROM clause
        let mut node = self.plan_from(from)?;
        let scan_table = extract_table_name(&node);

        // Check for index scan opportunity
        if let Some(ref predicate) = where_clause {
            if let Some(index_scan) = self.try_index_scan(predicate, &scan_table) {
                node = index_scan;
            } else {
                node = PlanNode::Filter {
                    predicate: predicate.clone(),
                    child: Box::new(node),
                };
            }
        }

        // Projection
        let needs_projection = !is_all_columns(&columns);
        if needs_projection {
            node = PlanNode::Projection {
                columns,
                child: Box::new(node),
            };
        }

        // Sort
        if !order_by.is_empty() {
            node = PlanNode::Sort {
                order_by,
                child: Box::new(node),
            };
        }

        // Limit
        if let Some(count) = limit {
            node = PlanNode::Limit {
                count,
                child: Box::new(node),
            };
        }

        Ok(node)
    }

    fn plan_from(&self, from: FromClause) -> Result<PlanNode> {
        match from {
            FromClause::Table { name, alias } => {
                // Verify table exists
                if self.catalog.get_table(&name).is_none() {
                    return Err(ForgeError::Plan(format!("table '{}' not found", name)));
                }
                Ok(PlanNode::SeqScan { table_name: name, alias })
            }
            FromClause::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let left_plan = self.plan_from(*left)?;
                let right_plan = self.plan_from(*right)?;
                Ok(PlanNode::NestedLoopJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    join_type,
                    on,
                })
            }
        }
    }

    fn plan_update(
        &self,
        table_name: String,
        assignments: Vec<Assignment>,
        where_clause: Option<Expr>,
    ) -> Result<PlanNode> {
        if self.catalog.get_table(&table_name).is_none() {
            return Err(ForgeError::Plan(format!(
                "table '{}' not found",
                table_name
            )));
        }

        let mut child: PlanNode = PlanNode::SeqScan {
            table_name: table_name.clone(),
            alias: None,
        };

        if let Some(predicate) = where_clause {
            child = PlanNode::Filter {
                predicate,
                child: Box::new(child),
            };
        }

        Ok(PlanNode::Update {
            table_name,
            assignments,
            child: Box::new(child),
        })
    }

    fn plan_delete(
        &self,
        table_name: String,
        where_clause: Option<Expr>,
    ) -> Result<PlanNode> {
        if self.catalog.get_table(&table_name).is_none() {
            return Err(ForgeError::Plan(format!(
                "table '{}' not found",
                table_name
            )));
        }

        let mut child: PlanNode = PlanNode::SeqScan {
            table_name: table_name.clone(),
            alias: None,
        };

        if let Some(predicate) = where_clause {
            child = PlanNode::Filter {
                predicate,
                child: Box::new(child),
            };
        }

        Ok(PlanNode::Delete {
            table_name,
            child: Box::new(child),
        })
    }

    /// Try to use an index scan for simple `column = value` predicates.
    fn try_index_scan(&self, predicate: &Expr, table_name: &Option<String>) -> Option<PlanNode> {
        let table = table_name.as_ref()?;

        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = predicate
        {
            // Check column = literal pattern
            if let Expr::ColumnRef { column, .. } = left.as_ref() {
                if matches!(right.as_ref(), Expr::Literal(_)) {
                    let index_key = format!("{}.{}", table.to_lowercase(), column.to_lowercase());
                    if self
                        .indexes
                        .iter()
                        .any(|(k, _)| k.to_lowercase() == index_key)
                    {
                        return Some(PlanNode::IndexScan {
                            table_name: table.clone(),
                            index_column: column.clone(),
                            lookup_value: *right.clone(),
                        });
                    }
                }
            }
        }

        None
    }
}

fn extract_table_name(node: &PlanNode) -> Option<String> {
    match node {
        PlanNode::SeqScan { table_name, .. } => Some(table_name.clone()),
        _ => None,
    }
}

fn is_all_columns(columns: &[SelectColumn]) -> bool {
    columns.len() == 1 && matches!(columns[0], SelectColumn::AllColumns(None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::common::PageId;
    use crate::tuple::schema::{Column, Schema};
    use crate::tuple::types::DataType;
    use tempfile::TempDir;

    fn test_catalog(dir: &TempDir) -> Catalog {
        let path = dir.path().join("test.catalog");
        let mut catalog = Catalog::new(path.to_str().unwrap());
        let schema = Schema::new(vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                column_id: 0,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Varchar(100),
                nullable: true,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
        ]);
        catalog
            .create_table("Users", schema, PageId(0))
            .unwrap();

        let schema2 = Schema::new(vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                column_id: 0,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
            Column {
                name: "user_id".into(),
                data_type: DataType::Integer,
                nullable: false,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
            Column {
                name: "total".into(),
                data_type: DataType::Float,
                nullable: true,
                column_id: 2,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
        ]);
        catalog
            .create_table("Orders", schema2, PageId(1))
            .unwrap();
        catalog
    }

    #[test]
    fn test_plan_create_table() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt = crate::sql::parse("CREATE TABLE Products (id INT NOT NULL)").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::CreateTable { .. }));
    }

    #[test]
    fn test_plan_drop_table() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt = crate::sql::parse("DROP TABLE Users").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::DropTable { .. }));
    }

    #[test]
    fn test_plan_select_simple() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt = crate::sql::parse("SELECT * FROM Users").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::SeqScan { .. }));
    }

    #[test]
    fn test_plan_select_with_where() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt = crate::sql::parse("SELECT * FROM Users WHERE id = 1").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::Filter { .. }));
    }

    #[test]
    fn test_plan_select_with_join() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt = crate::sql::parse(
            "SELECT * FROM Users u INNER JOIN Orders o ON u.id = o.user_id",
        )
        .unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::NestedLoopJoin { .. }));
    }

    #[test]
    fn test_plan_update_with_where() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt =
            crate::sql::parse("UPDATE Users SET name = 'Bob' WHERE id = 1").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::Update { .. }));
    }

    #[test]
    fn test_plan_delete_with_where() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt = crate::sql::parse("DELETE FROM Users WHERE id = 1").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::Delete { .. }));
    }

    #[test]
    fn test_plan_select_with_order_by_and_limit() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt =
            crate::sql::parse("SELECT TOP 5 * FROM Users ORDER BY name ASC").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::Limit { .. }));
    }
}
