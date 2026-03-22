use crate::catalog::Catalog;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::planner::cost_model;
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
                        table_name: String::new(),
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
                        table_name: String::new(),
                    });
                }
                Ok(PlanNode::DropTable { table_name })
            }
            Statement::Insert {
                table_name,
                columns,
                values,
                on_conflict,
            } => Ok(PlanNode::Insert {
                table_name,
                columns,
                values,
                on_conflict,
            }),
            Statement::Select {
                distinct,
                columns,
                from,
                r#where,
                group_by,
                having,
                order_by,
                limit,
                offset,
                ..
            } => self.plan_select(distinct, columns, from, r#where, group_by, having, order_by, limit, offset),
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
                include_columns,
            } => Ok(PlanNode::CreateIndex {
                index_name,
                table_name,
                columns,
                unique,
                include_columns,
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
            | Statement::AlterTable { .. }
            | Statement::Explain { .. }
            | Statement::CreateView { .. }
            | Statement::DropView { .. }
            | Statement::Union { .. }
            | Statement::AnalyzeTable { .. }
            | Statement::TruncateTable { .. }
            | Statement::InsertSelect { .. }
            | Statement::Savepoint { .. }
            | Statement::RollbackTo { .. }
            | Statement::ReleaseSavepoint { .. }
            | Statement::CreateSequence { .. }
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::CreateProcedure { .. }
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
                Err(ForgeError::Plan("statement type handled directly by database layer".into()))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_select(
        &self,
        distinct: bool,
        columns: Vec<SelectColumn>,
        from: FromClause,
        where_clause: Option<Expr>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        order_by: Vec<OrderByItem>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<PlanNode> {
        // Build scan from FROM clause
        let mut node = self.plan_from(from)?;
        let scan_table = extract_table_name(&node);

        // Check for index scan opportunity
        if let Some(ref predicate) = where_clause {
            if let Some(mut index_scan) = self.try_index_scan(predicate, &scan_table) {
                // Check for index-only scan: if the query only needs columns that
                // are in the index, we can skip the heap lookup entirely.
                if let PlanNode::IndexScan { ref index_column, ref mut index_only, ref table_name, .. } = index_scan {
                    if self.can_use_index_only_scan(table_name, index_column, &columns) {
                        *index_only = true;
                    }
                }
                node = index_scan;
            } else {
                node = PlanNode::Filter {
                    predicate: predicate.clone(),
                    child: Box::new(node),
                };
            }
        }

        // GROUP BY
        if !group_by.is_empty() {
            node = PlanNode::GroupBy {
                group_exprs: group_by,
                having,
                select_columns: columns.clone(),
                child: Box::new(node),
            };
            // After GROUP BY, add projection if needed but skip aggregate detection
            // (GroupBy node handles both grouping and projection)

            // DISTINCT
            if distinct {
                node = PlanNode::Distinct { child: Box::new(node) };
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
                    offset: offset.unwrap_or(0),
                    child: Box::new(node),
                };
            } else if let Some(off) = offset {
                if off > 0 {
                    node = PlanNode::Limit {
                        count: usize::MAX,
                        offset: off,
                        child: Box::new(node),
                    };
                }
            }

            return Ok(node);
        }

        // Projection (when no GROUP BY)
        let needs_projection = !is_all_columns(&columns);
        if needs_projection {
            node = PlanNode::Projection {
                columns,
                child: Box::new(node),
            };
        }

        // DISTINCT
        if distinct {
            node = PlanNode::Distinct { child: Box::new(node) };
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
                offset: offset.unwrap_or(0),
                child: Box::new(node),
            };
        } else if let Some(off) = offset {
            if off > 0 {
                node = PlanNode::Limit {
                    count: usize::MAX,
                    offset: off,
                    child: Box::new(node),
                };
            }
        }

        Ok(node)
    }

    fn plan_from(&self, from: FromClause) -> Result<PlanNode> {
        match from {
            FromClause::Table { name, alias } => {
                // Allow __dual__ for SELECT without FROM
                if name == "__dual__" {
                    return Ok(PlanNode::SeqScan { table_name: name, alias, parallel: false });
                }
                // Check stats to decide if parallel scan hint should be set
                let parallel = self.catalog.get_table(&name)
                    .and_then(|info| info.stats.as_ref())
                    .map(|stats| stats.row_count > 1000)
                    .unwrap_or(false);
                Ok(PlanNode::SeqScan { table_name: name, alias, parallel })
            }
            FromClause::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let left_plan = self.plan_from(*left)?;
                let right_plan = self.plan_from(*right)?;
                // Use cost model to choose between hash join and nested-loop join
                let use_hash_join = self.should_use_hash_join(&left_plan, &right_plan);
                if use_hash_join {
                    Ok(PlanNode::HashJoin {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        join_type,
                        on,
                    })
                } else {
                    Ok(PlanNode::NestedLoopJoin {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        join_type,
                        on,
                    })
                }
            }
            FromClause::Subquery { .. } => {
                // Derived tables handled at execution time
                Ok(PlanNode::SeqScan {
                    table_name: "__subquery__".to_string(),
                    alias: None,
                    parallel: false,
                })
            }
        }
    }

    /// Decide whether to use hash join based on cost model and table statistics.
    /// Returns true when both sides have statistics available and hash join is cheaper.
    fn should_use_hash_join(&self, left: &PlanNode, right: &PlanNode) -> bool {
        let left_stats = self.get_plan_stats(left);
        let right_stats = self.get_plan_stats(right);

        if let (Some((left_rows, left_pages)), Some((right_rows, _right_pages))) = (left_stats, right_stats) {
            let nlj_cost = cost_model::nested_loop_join_cost(left_rows, left_pages);
            let hj_cost = cost_model::hash_join_cost(left_rows, right_rows);
            hj_cost < nlj_cost
        } else {
            false // no stats, default to NLJ
        }
    }

    /// Extract (row_count, page_count) from a scan node's table stats.
    fn get_plan_stats(&self, plan: &PlanNode) -> Option<(u64, u64)> {
        match plan {
            PlanNode::SeqScan { table_name, .. } => {
                self.catalog.get_table(table_name)
                    .and_then(|info| info.stats.as_ref())
                    .map(|s| (s.row_count, s.page_count))
            }
            _ => None,
        }
    }

    /// Check if an index-only scan is possible: all selected columns must be
    /// covered by the index (key column + include columns).
    fn can_use_index_only_scan(
        &self,
        table_name: &str,
        index_column: &str,
        select_columns: &[SelectColumn],
    ) -> bool {
        // Only for simple queries selecting specific columns (not SELECT *)
        if select_columns.len() == 1 && matches!(select_columns[0], SelectColumn::AllColumns(_)) {
            return false;
        }

        // Find the index to check for include_columns
        let index_key = format!("{}.{}", table_name.to_lowercase(), index_column.to_lowercase());
        let index = self.indexes.iter().find(|(k, _)| k.to_lowercase() == index_key);

        // Collect all covered columns: index key + include columns
        let mut covered: Vec<String> = vec![index_column.to_lowercase()];
        if let Some((_, idx)) = index {
            for col in &idx.key_columns {
                let lc = col.to_lowercase();
                if !covered.contains(&lc) {
                    covered.push(lc);
                }
            }
            for col in &idx.include_columns {
                let lc = col.to_lowercase();
                if !covered.contains(&lc) {
                    covered.push(lc);
                }
            }
        }

        // Check that all referenced columns in the SELECT are covered
        for col in select_columns {
            match col {
                SelectColumn::Expr { expr: Expr::ColumnRef { column, .. }, .. } => {
                    if !covered.contains(&column.to_lowercase()) {
                        return false;
                    }
                }
                SelectColumn::AllColumns(_) => return false,
                _ => return false, // Complex expressions need full tuple
            }
        }

        true
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

        let scan_table = Some(table_name.clone());
        let mut child: PlanNode = PlanNode::SeqScan {
            table_name: table_name.clone(),
            alias: None,
            parallel: false,
        };

        if let Some(predicate) = where_clause {
            // Try index scan for UPDATE WHERE column = value
            if let Some(index_scan) = self.try_index_scan(&predicate, &scan_table) {
                child = index_scan;
            } else {
                child = PlanNode::Filter {
                    predicate,
                    child: Box::new(child),
                };
            }
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

        let scan_table = Some(table_name.clone());
        let mut child: PlanNode = PlanNode::SeqScan {
            table_name: table_name.clone(),
            alias: None,
            parallel: false,
        };

        if let Some(predicate) = where_clause {
            // Try index scan for DELETE WHERE column = value
            if let Some(index_scan) = self.try_index_scan(&predicate, &scan_table) {
                child = index_scan;
            } else {
                child = PlanNode::Filter {
                    predicate,
                    child: Box::new(child),
                };
            }
        }

        Ok(PlanNode::Delete {
            table_name,
            child: Box::new(child),
        })
    }

    /// Try to use an index scan for simple `column = value` predicates.
    /// Uses cost-based optimization when statistics are available.
    /// Also checks for composite index matches and index-only scan opportunities.
    fn try_index_scan(&self, predicate: &Expr, table_name: &Option<String>) -> Option<PlanNode> {
        let table = table_name.as_ref()?;

        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = predicate
        {
            if let Expr::ColumnRef { column, .. } = left.as_ref() {
                if matches!(right.as_ref(), Expr::Literal(_)) {
                    let index_key = format!("{}.{}", table.to_lowercase(), column.to_lowercase());
                    if self
                        .indexes
                        .iter()
                        .any(|(k, _)| k.to_lowercase() == index_key)
                    {
                        // If statistics are available, check if index scan is cheaper
                        if let Some(info) = self.catalog.get_table(table) {
                            if let (Some(table_stats), Some(col_stats)) =
                                (&info.stats, info.column_stats.get(&column.to_lowercase()))
                            {
                                if !cost_model::prefer_index_scan(col_stats, table_stats) {
                                    return None; // Sequential scan is cheaper
                                }
                            }
                        }
                        return Some(PlanNode::IndexScan {
                            table_name: table.clone(),
                            index_column: column.clone(),
                            lookup_value: *right.clone(),
                            index_only: false,
                        });
                    }

                    // Composite indexes (table.col1.col2) are not used for
                    // single-column predicates — they require all key columns.
                }
            }
        }

        // Try AND conditions for composite index matching
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = predicate
        {
            // Try each side individually
            if let Some(scan) = self.try_index_scan(left, table_name) {
                return Some(scan);
            }
            if let Some(scan) = self.try_index_scan(right, table_name) {
                return Some(scan);
            }
        }

        None
    }
}

fn extract_table_name(node: &PlanNode) -> Option<String> {
    match node {
        PlanNode::SeqScan { table_name, .. } => Some(table_name.clone()),
        PlanNode::HashJoin { left, .. } | PlanNode::NestedLoopJoin { left, .. } => {
            extract_table_name(left)
        }
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
                    is_unique: false,
                    check_expr: None, fk_ref: None,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Varchar(100),
                nullable: true,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                    is_unique: false,
                    check_expr: None, fk_ref: None,
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
                    is_unique: false,
                    check_expr: None, fk_ref: None,
            },
            Column {
                name: "user_id".into(),
                data_type: DataType::Integer,
                nullable: false,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                    is_unique: false,
                    check_expr: None, fk_ref: None,
            },
            Column {
                name: "total".into(),
                data_type: DataType::Float,
                nullable: true,
                column_id: 2,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                    is_unique: false,
                    check_expr: None, fk_ref: None,
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

    #[test]
    fn test_plan_group_by() {
        let dir = TempDir::new().unwrap();
        let catalog = test_catalog(&dir);
        let planner = Planner::new(&catalog, &[]);
        let stmt = crate::sql::parse("SELECT name, COUNT(*) FROM Users GROUP BY name").unwrap();
        let plan = planner.plan(stmt).unwrap();
        assert!(matches!(plan, PlanNode::GroupBy { .. }));
    }
}
