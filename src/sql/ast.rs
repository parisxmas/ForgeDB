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
        on_conflict: Option<OnConflict>,
    },
    Select {
        distinct: bool,
        columns: Vec<SelectColumn>,
        from: FromClause,
        r#where: Option<Expr>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        order_by: Vec<OrderByItem>,
        limit: Option<usize>,
        offset: Option<usize>,
        ctes: Vec<Cte>,
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
        /// Covering index: additional columns stored in the index for index-only scans.
        /// Populated from `CREATE INDEX ... ON table(key) INCLUDE (col1, col2)`.
        include_columns: Vec<String>,
    },
    AlterTable {
        table_name: String,
        operations: Vec<AlterTableOp>,
    },
    /// EXPLAIN <statement>
    Explain {
        statement: Box<Statement>,
    },
    /// CREATE VIEW name [(columns)] AS <select>
    CreateView {
        name: String,
        column_aliases: Option<Vec<String>>,
        query: Box<Statement>,
    },
    /// DROP VIEW [IF EXISTS] name
    DropView {
        name: String,
        if_exists: bool,
    },
    /// UNION / UNION ALL of multiple selects
    Union {
        left: Box<Statement>,
        right: Box<Statement>,
        all: bool,
    },
    /// ANALYZE TABLE — gather statistics for cost-based optimizer
    AnalyzeTable {
        table_name: String,
    },
    /// TRUNCATE TABLE
    TruncateTable {
        table_name: String,
    },
    /// INSERT INTO ... SELECT ...
    InsertSelect {
        table_name: String,
        columns: Option<Vec<String>>,
        query: Box<Statement>,
    },
    /// SAVEPOINT name
    Savepoint {
        name: String,
    },
    /// ROLLBACK TO SAVEPOINT name
    RollbackTo {
        name: String,
    },
    /// RELEASE SAVEPOINT name
    ReleaseSavepoint {
        name: String,
    },
    /// CREATE SEQUENCE
    CreateSequence {
        name: String,
        start: i64,
        increment: i64,
    },
    /// CREATE DATABASE
    CreateDatabase {
        name: String,
    },
    /// DROP DATABASE
    DropDatabase {
        name: String,
    },
    /// CREATE PROCEDURE name (params) AS BEGIN ... END
    CreateProcedure {
        name: String,
        params: Vec<(String, DataType)>,
        body: Vec<String>,
    },
    /// EXEC / EXECUTE procedure_name [args]
    ExecProcedure {
        name: String,
        args: Vec<Expr>,
    },
    /// CREATE TRIGGER name ON table AFTER event AS BEGIN ... END
    CreateTrigger {
        name: String,
        table: String,
        event: TriggerEvent,
        body: Vec<String>,
    },
    /// CREATE USER name WITH PASSWORD 'password'
    CreateUser {
        name: String,
        password: String,
    },
    /// DROP USER name
    DropUser {
        name: String,
    },
    /// GRANT privilege ON table TO user
    Grant {
        privilege: String,
        on_table: Option<String>,
        to_user: String,
    },
    /// REVOKE privilege ON table FROM user
    Revoke {
        privilege: String,
        on_table: Option<String>,
        from_user: String,
    },
    /// PREPARE name AS sql
    Prepare {
        name: String,
        sql: String,
    },
    /// EXECUTE prepared_name
    ExecutePrepared {
        name: String,
        params: Vec<Expr>,
    },
    /// BACKUP DATABASE TO 'path'
    Backup {
        path: String,
    },
    /// RESTORE DATABASE FROM 'path'
    Restore {
        path: String,
    },
    /// DECLARE cursor_name CURSOR FOR select_sql
    DeclareCursor {
        name: String,
        query_sql: String,
    },
    /// OPEN cursor_name
    OpenCursor {
        name: String,
    },
    /// FETCH NEXT FROM cursor_name
    FetchCursor {
        name: String,
    },
    /// CLOSE cursor_name
    CloseCursor {
        name: String,
    },
    /// DEALLOCATE cursor_name
    DeallocateCursor {
        name: String,
    },
}

/// Trigger events
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    AfterInsert,
    AfterUpdate,
    AfterDelete,
    BeforeInsert,
    BeforeUpdate,
    BeforeDelete,
}

/// Trigger definition stored in the database
#[derive(Debug, Clone)]
pub struct TriggerDef {
    pub name: String,
    pub table: String,
    pub event: TriggerEvent,
    pub body: Vec<String>,
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
    pub is_unique: bool,
    pub check_expr: Option<Expr>,
    pub references: Option<ForeignKeyRef>,
}

/// Action to take on DELETE of referenced row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FkAction {
    Restrict,  // default: reject delete
    Cascade,   // delete child rows
    SetNull,   // set FK column to NULL
}

/// Foreign key reference for a column.
#[derive(Debug, Clone)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
    pub on_delete: FkAction,
}

/// ON CONFLICT / ON DUPLICATE KEY clause for INSERT.
#[derive(Debug, Clone)]
pub struct OnConflict {
    pub columns: Vec<String>,
    pub action: OnConflictAction,
}

/// Action to take when a duplicate key is found.
#[derive(Debug, Clone)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate(Vec<Assignment>),
}

/// Common Table Expression (WITH ... AS ...)
#[derive(Debug, Clone)]
pub struct Cte {
    pub name: String,
    pub query: Box<Statement>,
    pub recursive: bool,
}

/// Operations for ALTER TABLE.
#[derive(Debug, Clone)]
pub enum AlterTableOp {
    AddColumn(ColumnDef),
    DropColumn(String),
    ModifyColumn(ColumnDef),
    RenameColumn { old_name: String, new_name: String },
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
        on: Option<Expr>,
    },
    /// Derived table: (SELECT ...) AS alias
    Subquery {
        query: Box<Statement>,
        alias: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
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
    /// Function call.
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
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
    /// `expr IN (pre-computed value set)` — O(1) hash lookup.
    /// Used when subquery results are inlined. The `keys` HashSet contains
    /// sort-key-encoded values for O(1) membership testing.
    InValues {
        expr: Box<Expr>,
        values: Vec<crate::tuple::types::Value>,
        keys: std::collections::HashSet<Vec<u8>>,
        negated: bool,
    },
    /// `expr BETWEEN low AND high`
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// CASE [operand] WHEN cond THEN result [...] [ELSE default] END
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// Scalar subquery: (SELECT ...)
    Subquery(Box<Statement>),
    /// expr IN (SELECT ...)
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Statement>,
        negated: bool,
    },
    /// EXISTS (SELECT ...)
    Exists {
        subquery: Box<Statement>,
        negated: bool,
    },
    /// CAST(expr AS type)
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// Window function: func OVER (PARTITION BY ... ORDER BY ...)
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByItem>,
    },
    /// String concatenation with || operator
    Concat {
        left: Box<Expr>,
        right: Box<Expr>,
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
    Modulo,
}

/// Unary operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Neg,
}
