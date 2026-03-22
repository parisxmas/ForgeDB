use std::fmt;
use crate::sql::ast::{Assignment, ColumnDef, Expr, JoinType, OnConflict, OrderByItem, SelectColumn};

/// Physical plan node. Each variant carries everything needed for execution.
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Sequential scan of a table.
    SeqScan {
        table_name: String,
        alias: Option<String>,
        /// Hint: true when table stats show > 1000 rows, enabling batch-page scanning.
        parallel: bool,
    },

    /// Index scan using a B-tree index.
    IndexScan {
        table_name: String,
        index_column: String,
        lookup_value: Expr,
        /// True when all columns needed by the query are in the index, allowing
        /// the executor to skip the heap lookup entirely.
        index_only: bool,
    },

    /// Hash join — chosen by the planner when cost model shows it is cheaper
    /// than nested-loop join.
    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        on: Option<Expr>,
    },

    /// Filter rows (WHERE clause).
    Filter {
        predicate: Expr,
        child: Box<PlanNode>,
    },

    /// Project specific columns.
    Projection {
        columns: Vec<SelectColumn>,
        child: Box<PlanNode>,
    },

    /// Nested-loop join.
    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        on: Option<Expr>,
    },

    /// ORDER BY.
    Sort {
        order_by: Vec<OrderByItem>,
        child: Box<PlanNode>,
    },

    /// LIMIT / TOP with optional OFFSET.
    Limit {
        count: usize,
        offset: usize,
        child: Box<PlanNode>,
    },

    /// INSERT INTO table VALUES (...)
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
        on_conflict: Option<OnConflict>,
    },

    /// UPDATE table SET ... WHERE ...
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        child: Box<PlanNode>,
    },

    /// DELETE FROM table WHERE ...
    Delete {
        table_name: String,
        child: Box<PlanNode>,
    },

    /// CREATE TABLE
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
    },

    /// DROP TABLE
    DropTable {
        table_name: String,
    },

    /// CREATE INDEX
    CreateIndex {
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
        /// Covering index: non-key columns stored in the index for index-only scans.
        include_columns: Vec<String>,
    },

    /// GROUP BY with optional HAVING and aggregate expressions
    GroupBy {
        group_exprs: Vec<Expr>,
        having: Option<Expr>,
        select_columns: Vec<SelectColumn>,
        child: Box<PlanNode>,
    },

    /// SELECT DISTINCT
    Distinct {
        child: Box<PlanNode>,
    },
}

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indent(f, 0)
    }
}

impl PlanNode {
    fn fmt_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let pad = "  ".repeat(indent);
        match self {
            PlanNode::SeqScan { table_name, alias, parallel } => {
                write!(f, "{}SeqScan: {}", pad, table_name)?;
                if let Some(a) = alias { write!(f, " AS {}", a)?; }
                if *parallel { write!(f, " [parallel]")?; }
                writeln!(f)
            }
            PlanNode::IndexScan { table_name, index_column, index_only, .. } => {
                if *index_only {
                    writeln!(f, "{}IndexOnlyScan: {}.{}", pad, table_name, index_column)
                } else {
                    writeln!(f, "{}IndexScan: {}.{}", pad, table_name, index_column)
                }
            }
            PlanNode::HashJoin { left, right, join_type, .. } => {
                writeln!(f, "{}{:?} HashJoin:", pad, join_type)?;
                left.fmt_indent(f, indent + 1)?;
                right.fmt_indent(f, indent + 1)
            }
            PlanNode::Filter { child, .. } => {
                writeln!(f, "{}Filter:", pad)?;
                child.fmt_indent(f, indent + 1)
            }
            PlanNode::Projection { columns, child } => {
                writeln!(f, "{}Projection: {} columns", pad, columns.len())?;
                child.fmt_indent(f, indent + 1)
            }
            PlanNode::NestedLoopJoin { left, right, join_type, .. } => {
                writeln!(f, "{}{:?} Join:", pad, join_type)?;
                left.fmt_indent(f, indent + 1)?;
                right.fmt_indent(f, indent + 1)
            }
            PlanNode::Sort { order_by, child } => {
                writeln!(f, "{}Sort: {} keys", pad, order_by.len())?;
                child.fmt_indent(f, indent + 1)
            }
            PlanNode::Limit { count, offset, child } => {
                if *offset > 0 {
                    writeln!(f, "{}Limit: {} Offset: {}", pad, count, offset)?;
                } else {
                    writeln!(f, "{}Limit: {}", pad, count)?;
                }
                child.fmt_indent(f, indent + 1)
            }
            PlanNode::Insert { table_name, values, .. } => {
                writeln!(f, "{}Insert: {} ({} rows)", pad, table_name, values.len())
            }
            PlanNode::Update { table_name, .. } => {
                writeln!(f, "{}Update: {}", pad, table_name)
            }
            PlanNode::Delete { table_name, .. } => {
                writeln!(f, "{}Delete: {}", pad, table_name)
            }
            PlanNode::CreateTable { table_name, columns } => {
                writeln!(f, "{}CreateTable: {} ({} columns)", pad, table_name, columns.len())
            }
            PlanNode::DropTable { table_name } => {
                writeln!(f, "{}DropTable: {}", pad, table_name)
            }
            PlanNode::CreateIndex { index_name, table_name, .. } => {
                writeln!(f, "{}CreateIndex: {} ON {}", pad, index_name, table_name)
            }
            PlanNode::GroupBy { group_exprs, child, .. } => {
                writeln!(f, "{}GroupBy: {} keys", pad, group_exprs.len())?;
                child.fmt_indent(f, indent + 1)
            }
            PlanNode::Distinct { child } => {
                writeln!(f, "{}Distinct:", pad)?;
                child.fmt_indent(f, indent + 1)
            }
        }
    }
}
