use crate::sql::ast::{Assignment, ColumnDef, Expr, JoinType, OrderByItem, SelectColumn};

/// Physical plan node. Each variant carries everything needed for execution.
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Sequential scan of a table.
    SeqScan {
        table_name: String,
        alias: Option<String>,
    },

    /// Index scan using a B-tree index.
    IndexScan {
        table_name: String,
        index_column: String,
        lookup_value: Expr,
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
        on: Expr,
    },

    /// ORDER BY.
    Sort {
        order_by: Vec<OrderByItem>,
        child: Box<PlanNode>,
    },

    /// LIMIT / TOP.
    Limit {
        count: usize,
        child: Box<PlanNode>,
    },

    /// INSERT INTO table VALUES (...)
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
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
    },
}
