use crate::tuple::types::DataType;

/// Top-level SQL statement.
#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    },
    Select {
        columns: Vec<SelectColumn>,
        from: FromClause,
        r#where: Option<Expr>,
        order_by: Vec<OrderByItem>,
        limit: Option<usize>,
    },
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        r#where: Option<Expr>,
    },
    Delete {
        table_name: String,
        r#where: Option<Expr>,
    },
    ShowTables,
    ShowColumns {
        table_name: String,
    },
    ShowCreateTable {
        table_name: String,
    },
    DescribeTable {
        table_name: String,
    },
    SetVariable {
        name: String,
        value: Expr,
    },
    UseDatabase {
        name: String,
    },
    StartTransaction,
    Commit,
    Rollback,
    CreateIndex {
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
    },
    AlterTable {
        table_name: String,
        operations: Vec<AlterTableOp>,
    },
}

/// A column definition in CREATE TABLE.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub auto_increment: bool,
    pub default_value: Option<Expr>,
    pub is_primary_key: bool,
}

/// Operations for ALTER TABLE.
#[derive(Debug, Clone)]
pub enum AlterTableOp {
    AddColumn(ColumnDef),
    DropColumn(String),
    AddIndex {
        index_name: Option<String>,
        columns: Vec<String>,
        unique: bool,
    },
}

/// A column in a SELECT list.
#[derive(Debug, Clone)]
pub enum SelectColumn {
    /// `*` or `table.*`
    AllColumns(Option<String>),
    /// An expression with optional alias.
    Expr { expr: Expr, alias: Option<String> },
}

/// FROM clause.
#[derive(Debug, Clone)]
pub enum FromClause {
    Table { name: String, alias: Option<String> },
    Join {
        left: Box<FromClause>,
        right: Box<FromClause>,
        join_type: JoinType,
        on: Expr,
    },
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// SET assignment in UPDATE.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

/// ORDER BY item.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
}

/// Expression tree.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A literal value.
    Literal(LiteralValue),
    /// A column reference, optionally qualified: `table.column` or just `column`.
    ColumnRef {
        table: Option<String>,
        column: String,
    },
    /// Binary operation: `left op right`.
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation: `op expr` (e.g., NOT, negation).
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// `expr IS NULL`
    IsNull(Box<Expr>),
    /// `expr IS NOT NULL`
    IsNotNull(Box<Expr>),
    /// Function call (for future extensibility).
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// `expr LIKE pattern`
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
    },
    /// `expr NOT LIKE pattern`
    NotLike {
        expr: Box<Expr>,
        pattern: Box<Expr>,
    },
    /// `expr IN (list)`
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    /// `expr NOT IN (list)`
    NotIn {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    /// `expr BETWEEN low AND high`
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
}

/// Literal values in SQL expressions.
#[derive(Debug, Clone)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

/// Binary operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
}

/// Unary operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Neg,
}
