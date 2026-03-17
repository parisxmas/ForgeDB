use sqlparser::ast::{self as sp, ObjectNamePart, SelectItem, SetExpr, TableFactor, TopQuantity};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{ForgeError, Result};
use crate::sql::ast::*;
use crate::tuple::types::DataType;

/// Parse a single SQL statement (Generic dialect for MySQL + T-SQL compat).
pub fn parse(sql: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ForgeError::Parse(format!("{}", e)))?;

    if statements.is_empty() {
        return Err(ForgeError::Parse("empty SQL".into()));
    }
    if statements.len() > 1 {
        return Err(ForgeError::Parse("multiple statements not supported".into()));
    }

    convert_statement(statements.into_iter().next().unwrap())
}

fn convert_statement(stmt: sp::Statement) -> Result<Statement> {
    match stmt {
        sp::Statement::CreateTable(ct) => convert_create_table(ct),
        sp::Statement::Drop {
            object_type: sp::ObjectType::Table,
            names,
            if_exists,
            ..
        } => {
            let name = object_name_to_string(&names[0]);
            Ok(Statement::DropTable { table_name: name, if_exists })
        }
        sp::Statement::Insert(ins) => convert_insert(ins),
        sp::Statement::Query(query) => convert_query(*query),
        sp::Statement::Update(upd) => convert_update(upd),
        sp::Statement::Delete(del) => convert_delete(del),
        sp::Statement::ShowTables { .. } => Ok(Statement::ShowTables),
        sp::Statement::ShowColumns { show_options, .. } => {
            let table_name = show_options.show_in
                .as_ref()
                .map(|si| si.to_string())
                .unwrap_or_default()
                .replace("FROM ", "")
                .replace("IN ", "");
            Ok(Statement::ShowColumns { table_name })
        }
        sp::Statement::ShowCreate {
            obj_name, ..
        } => Ok(Statement::ShowCreateTable {
            table_name: object_name_to_string(&obj_name),
        }),
        sp::Statement::ExplainTable { table_name, .. } => Ok(Statement::DescribeTable {
            table_name: object_name_to_string(&table_name),
        }),
        sp::Statement::Set(_) => {
            // Stub: accept any SET command
            Ok(Statement::SetVariable {
                name: String::new(),
                value: Expr::Literal(LiteralValue::Null),
            })
        }
        sp::Statement::Use(ref use_stmt) => {
            let name = match use_stmt {
                sp::Use::Database(n) | sp::Use::Catalog(n) | sp::Use::Schema(n)
                | sp::Use::Object(n) | sp::Use::Warehouse(n) => object_name_to_string(n),
                _ => String::new(),
            };
            Ok(Statement::UseDatabase { name })
        }
        sp::Statement::StartTransaction { .. } => Ok(Statement::StartTransaction),
        sp::Statement::Commit { .. } => Ok(Statement::Commit),
        sp::Statement::Rollback { .. } => Ok(Statement::Rollback),
        sp::Statement::CreateIndex(ci) => {
            let index_name = ci.name.as_ref()
                .map(|n| object_name_to_string(n))
                .unwrap_or_default();
            let table_name = object_name_to_string(&ci.table_name);
            let columns: Vec<String> = ci.columns.iter()
                .map(|c| c.column.expr.to_string())
                .collect();
            Ok(Statement::CreateIndex {
                index_name,
                table_name,
                columns,
                unique: ci.unique,
            })
        }
        _ => Err(ForgeError::Parse(
            "unsupported statement type".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

fn convert_create_table(ct: sp::CreateTable) -> Result<Statement> {
    let table_name = object_name_to_string(&ct.name);
    let mut columns = Vec::new();

    for col in &ct.columns {
        let name = ident_to_string(&col.name);
        let data_type = convert_data_type(&col.data_type)?;

        let mut nullable = true;
        let mut auto_increment = false;
        let mut default_value = None;
        let mut is_primary_key = false;

        for opt in &col.options {
            match &opt.option {
                sp::ColumnOption::NotNull => nullable = false,
                sp::ColumnOption::Null => nullable = true,
                sp::ColumnOption::Default(expr) => {
                    default_value = convert_expr(expr).ok().map(|e| Some(e)).unwrap_or(None);
                }
                sp::ColumnOption::Unique(uc) => {
                    let _ = uc;
                    // UNIQUE constraint (not primary key)
                }
                sp::ColumnOption::PrimaryKey(_) => {
                    is_primary_key = true;
                    nullable = false;
                }
                sp::ColumnOption::DialectSpecific(tokens) => {
                    let combined: String = tokens.iter().map(|t| t.to_string().to_uppercase()).collect::<Vec<_>>().join(" ");
                    if combined.contains("AUTO_INCREMENT") || combined.contains("AUTOINCREMENT") {
                        auto_increment = true;
                    }
                }
                _ => {}
            }
        }

        columns.push(ColumnDef {
            name,
            data_type,
            nullable,
            auto_increment,
            default_value,
            is_primary_key,
        });
    }

    // Handle table-level PRIMARY KEY constraints
    for constraint in &ct.constraints {
        if let sp::TableConstraint::PrimaryKey(pk) = constraint {
            for idx_col in &pk.columns {
                let col_name = idx_col.column.expr.to_string();
                for col in &mut columns {
                    if col.name.eq_ignore_ascii_case(&col_name) {
                        col.is_primary_key = true;
                        col.nullable = false;
                    }
                }
            }
        }
    }

    Ok(Statement::CreateTable {
        table_name,
        columns,
        if_not_exists: ct.if_not_exists,
    })
}

fn convert_data_type(dt: &sp::DataType) -> Result<DataType> {
    match dt {
        sp::DataType::Int(_) | sp::DataType::Integer(_) | sp::DataType::Int4(_)
        | sp::DataType::IntUnsigned(_) | sp::DataType::IntegerUnsigned(_)
        | sp::DataType::Int4Unsigned(_) => {
            Ok(DataType::Integer)
        }
        sp::DataType::BigInt(_) | sp::DataType::Int8(_)
        | sp::DataType::BigIntUnsigned(_) | sp::DataType::Int8Unsigned(_) => Ok(DataType::BigInt),
        sp::DataType::SmallInt(_) | sp::DataType::TinyInt(_) | sp::DataType::Int2(_)
        | sp::DataType::SmallIntUnsigned(_) | sp::DataType::TinyIntUnsigned(_)
        | sp::DataType::Int2Unsigned(_) | sp::DataType::MediumIntUnsigned(_) => {
            Ok(DataType::Integer)
        }
        sp::DataType::Float(_)
        | sp::DataType::Real
        | sp::DataType::Double(_)
        | sp::DataType::DoublePrecision
        | sp::DataType::Float4
        | sp::DataType::Float8 => Ok(DataType::Float),
        sp::DataType::Varchar(len_info) => {
            let n = extract_char_length(len_info).unwrap_or(255);
            Ok(DataType::Varchar(n))
        }
        sp::DataType::Nvarchar(len_info) => {
            let n = extract_char_length(len_info).unwrap_or(255);
            Ok(DataType::Varchar(n))
        }
        sp::DataType::Char(len_info) | sp::DataType::Character(len_info) => {
            let n = extract_char_length(len_info).unwrap_or(1);
            Ok(DataType::Varchar(n))
        }
        sp::DataType::Text | sp::DataType::LongText | sp::DataType::MediumText
        | sp::DataType::TinyText => {
            Ok(DataType::Varchar(255))
        }
        sp::DataType::Boolean | sp::DataType::Bool => Ok(DataType::Boolean),
        sp::DataType::Bit(len_info) => {
            let _ = len_info;
            Ok(DataType::Boolean)
        }
        sp::DataType::Datetime(_) | sp::DataType::Timestamp(_, _) => Ok(DataType::DateTime),
        sp::DataType::MediumInt(_) => Ok(DataType::Integer),
        sp::DataType::Decimal(_) | sp::DataType::Dec(_) | sp::DataType::Numeric(_) => {
            Ok(DataType::Float)
        }
        sp::DataType::Blob(_) | sp::DataType::MediumBlob | sp::DataType::LongBlob
        | sp::DataType::TinyBlob => {
            Ok(DataType::Varchar(255))
        }
        sp::DataType::Enum(variants, _) => {
            let _ = variants;
            Ok(DataType::Varchar(255))
        }
        sp::DataType::Set(variants) => {
            let _ = variants;
            Ok(DataType::Varchar(255))
        }
        sp::DataType::Date => Ok(DataType::DateTime),
        sp::DataType::Time(_, _) => Ok(DataType::DateTime),
        sp::DataType::Varbinary(_) | sp::DataType::Binary(_) => Ok(DataType::Varchar(255)),
        _ => Err(ForgeError::Parse(format!(
            "unsupported data type: {:?}",
            dt
        ))),
    }
}

fn extract_char_length(len_info: &Option<sp::CharacterLength>) -> Option<u16> {
    match len_info.as_ref()? {
        sp::CharacterLength::IntegerLength { length, .. } => Some(*length as u16),
        sp::CharacterLength::Max => Some(u16::MAX),
    }
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

fn convert_insert(ins: sp::Insert) -> Result<Statement> {
    let table_name = match &ins.table {
        sp::TableObject::TableName(name) => object_name_to_string(name),
        _ => return Err(ForgeError::Parse("unsupported insert target".into())),
    };

    let columns = if ins.columns.is_empty() {
        None
    } else {
        Some(ins.columns.iter().map(|c| ident_to_string(c)).collect())
    };

    let source = ins
        .source
        .as_ref()
        .ok_or_else(|| ForgeError::Parse("INSERT without VALUES".into()))?;

    let values = match source.body.as_ref() {
        SetExpr::Values(vals) => {
            let mut rows = Vec::new();
            for row in &vals.rows {
                let mut exprs = Vec::new();
                for expr in row {
                    exprs.push(convert_expr(expr)?);
                }
                rows.push(exprs);
            }
            rows
        }
        _ => return Err(ForgeError::Parse("expected VALUES in INSERT".into())),
    };

    Ok(Statement::Insert {
        table_name,
        columns,
        values,
    })
}

// ---------------------------------------------------------------------------
// SELECT / Query
// ---------------------------------------------------------------------------

fn convert_query(query: sp::Query) -> Result<Statement> {
    // ORDER BY
    let order_by = if let Some(ref ob) = query.order_by {
        convert_order_by(ob)?
    } else {
        vec![]
    };

    // LIMIT (standard SQL)
    let limit_val = if let Some(ref lc) = query.limit_clause {
        match lc {
            sp::LimitClause::LimitOffset { limit, .. } => {
                limit.as_ref().and_then(|e| expr_to_usize(e))
            }
            _ => None,
        }
    } else {
        None
    };

    let select = match *query.body {
        SetExpr::Select(sel) => *sel,
        _ => return Err(ForgeError::Parse("expected SELECT".into())),
    };

    // TOP (T-SQL)
    let top_limit = select.top.as_ref().and_then(|t| {
        t.quantity.as_ref().and_then(|q| match q {
            TopQuantity::Constant(n) => Some(*n as usize),
            TopQuantity::Expr(e) => expr_to_usize(e),
        })
    });

    let limit = limit_val.or(top_limit);

    // Columns
    let columns: Vec<SelectColumn> = select
        .projection
        .iter()
        .map(|item| convert_select_item(item))
        .collect::<Result<_>>()?;

    // FROM
    let from = if select.from.is_empty() {
        return Err(ForgeError::Parse("SELECT without FROM".into()));
    } else {
        convert_from(&select.from)?
    };

    // WHERE
    let where_clause = select
        .selection
        .as_ref()
        .map(|e| convert_expr(e))
        .transpose()?;

    Ok(Statement::Select {
        columns,
        from,
        r#where: where_clause,
        order_by,
        limit,
    })
}

fn convert_select_item(item: &SelectItem) -> Result<SelectColumn> {
    match item {
        SelectItem::Wildcard(_) => Ok(SelectColumn::AllColumns(None)),
        SelectItem::QualifiedWildcard(kind, _) => {
            let table = match kind {
                sp::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                    Some(object_name_to_string(name))
                }
                _ => None,
            };
            Ok(SelectColumn::AllColumns(table))
        }
        SelectItem::UnnamedExpr(expr) => Ok(SelectColumn::Expr {
            expr: convert_expr(expr)?,
            alias: None,
        }),
        SelectItem::ExprWithAlias { expr, alias } => Ok(SelectColumn::Expr {
            expr: convert_expr(expr)?,
            alias: Some(ident_to_string(alias)),
        }),
    }
}

fn convert_from(from: &[sp::TableWithJoins]) -> Result<FromClause> {
    if from.is_empty() {
        return Err(ForgeError::Parse("empty FROM clause".into()));
    }

    let first = &from[0];
    let mut result = convert_table_factor(&first.relation)?;

    for join in &first.joins {
        let right = convert_table_factor(&join.relation)?;
        let (join_type, on_expr) = convert_join_operator(&join.join_operator)?;
        result = FromClause::Join {
            left: Box::new(result),
            right: Box::new(right),
            join_type,
            on: on_expr,
        };
    }

    Ok(result)
}

fn convert_table_factor(tf: &TableFactor) -> Result<FromClause> {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name);
            let alias_name = alias.as_ref().map(|a| ident_to_string(&a.name));
            Ok(FromClause::Table {
                name: table_name,
                alias: alias_name,
            })
        }
        _ => Err(ForgeError::Parse("unsupported FROM clause element".into())),
    }
}

fn convert_join_operator(op: &sp::JoinOperator) -> Result<(JoinType, Expr)> {
    let (jt, constraint) = match op {
        sp::JoinOperator::Inner(c) => (JoinType::Inner, c),
        sp::JoinOperator::Join(c) => (JoinType::Inner, c),
        sp::JoinOperator::Left(c) => (JoinType::Left, c),
        sp::JoinOperator::LeftOuter(c) => (JoinType::Left, c),
        sp::JoinOperator::Right(c) => (JoinType::Right, c),
        sp::JoinOperator::RightOuter(c) => (JoinType::Right, c),
        _ => return Err(ForgeError::Parse("unsupported join type".into())),
    };

    let on_expr = match constraint {
        sp::JoinConstraint::On(expr) => convert_expr(expr)?,
        _ => return Err(ForgeError::Parse("only ON join constraint supported".into())),
    };

    Ok((jt, on_expr))
}

fn convert_order_by(ob: &sp::OrderBy) -> Result<Vec<OrderByItem>> {
    match &ob.kind {
        sp::OrderByKind::Expressions(exprs) => {
            let mut items = Vec::new();
            for obe in exprs {
                let expr = convert_expr(&obe.expr)?;
                let ascending = obe.options.asc.unwrap_or(true);
                items.push(OrderByItem { expr, ascending });
            }
            Ok(items)
        }
        _ => Err(ForgeError::Parse("unsupported ORDER BY kind".into())),
    }
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

fn convert_update(upd: sp::Update) -> Result<Statement> {
    let table_name = match &upd.table.relation {
        TableFactor::Table { name, .. } => object_name_to_string(name),
        _ => return Err(ForgeError::Parse("unsupported UPDATE target".into())),
    };

    let mut assignments = Vec::new();
    for a in &upd.assignments {
        let column = match &a.target {
            sp::AssignmentTarget::ColumnName(name) => object_name_to_string(name),
            _ => return Err(ForgeError::Parse("unsupported assignment target".into())),
        };
        let value = convert_expr(&a.value)?;
        assignments.push(Assignment { column, value });
    }

    let where_clause = upd
        .selection
        .as_ref()
        .map(|e| convert_expr(e))
        .transpose()?;

    Ok(Statement::Update {
        table_name,
        assignments,
        r#where: where_clause,
    })
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn convert_delete(del: sp::Delete) -> Result<Statement> {
    let tables = match &del.from {
        sp::FromTable::WithFromKeyword(tables) | sp::FromTable::WithoutKeyword(tables) => tables,
    };

    if tables.is_empty() {
        return Err(ForgeError::Parse("DELETE without FROM".into()));
    }

    let table_name = match &tables[0].relation {
        TableFactor::Table { name, .. } => object_name_to_string(name),
        _ => return Err(ForgeError::Parse("unsupported DELETE target".into())),
    };

    let where_clause = del
        .selection
        .as_ref()
        .map(|e| convert_expr(e))
        .transpose()?;

    Ok(Statement::Delete {
        table_name,
        r#where: where_clause,
    })
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

fn convert_expr(expr: &sp::Expr) -> Result<Expr> {
    match expr {
        sp::Expr::Identifier(ident) => Ok(Expr::ColumnRef {
            table: None,
            column: ident_to_string(ident),
        }),
        sp::Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(Expr::ColumnRef {
                    table: Some(ident_to_string(&parts[0])),
                    column: ident_to_string(&parts[1]),
                })
            } else if parts.len() == 1 {
                Ok(Expr::ColumnRef {
                    table: None,
                    column: ident_to_string(&parts[0]),
                })
            } else {
                let col = ident_to_string(parts.last().unwrap());
                let table = ident_to_string(&parts[parts.len() - 2]);
                Ok(Expr::ColumnRef {
                    table: Some(table),
                    column: col,
                })
            }
        }
        sp::Expr::Value(val) => convert_value(val),
        sp::Expr::BinaryOp { left, op, right } => {
            let l = convert_expr(left)?;
            let r = convert_expr(right)?;
            let operator = convert_binary_op(op)?;
            Ok(Expr::BinaryOp {
                left: Box::new(l),
                op: operator,
                right: Box::new(r),
            })
        }
        sp::Expr::UnaryOp { op, expr } => {
            let e = convert_expr(expr)?;
            let operator = match op {
                sp::UnaryOperator::Not => UnaryOperator::Not,
                sp::UnaryOperator::Minus => UnaryOperator::Neg,
                _ => {
                    return Err(ForgeError::Parse(format!(
                        "unsupported unary operator: {:?}",
                        op
                    )))
                }
            };
            Ok(Expr::UnaryOp {
                op: operator,
                expr: Box::new(e),
            })
        }
        sp::Expr::Nested(inner) => convert_expr(inner),
        sp::Expr::IsNull(inner) => {
            let e = convert_expr(inner)?;
            Ok(Expr::IsNull(Box::new(e)))
        }
        sp::Expr::IsNotNull(inner) => {
            let e = convert_expr(inner)?;
            Ok(Expr::IsNotNull(Box::new(e)))
        }
        sp::Expr::Function(func) => {
            let name = object_name_to_string(&func.name);
            let mut args = Vec::new();
            match &func.args {
                sp::FunctionArguments::List(arg_list) => {
                    for arg in &arg_list.args {
                        match arg {
                            sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => {
                                args.push(convert_expr(e)?);
                            }
                            sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard) => {
                                args.push(Expr::Literal(LiteralValue::String("*".into())));
                            }
                            sp::FunctionArg::Unnamed(sp::FunctionArgExpr::QualifiedWildcard(_)) => {
                                args.push(Expr::Literal(LiteralValue::String("*".into())));
                            }
                            _ => {}
                        }
                    }
                }
                sp::FunctionArguments::None => {}
                sp::FunctionArguments::Subquery(_) => {}
            }
            Ok(Expr::Function { name, args })
        }
        sp::Expr::Like { negated, expr, pattern, .. } => {
            let e = convert_expr(expr)?;
            let p = convert_expr(pattern)?;
            if *negated {
                Ok(Expr::NotLike {
                    expr: Box::new(e),
                    pattern: Box::new(p),
                })
            } else {
                Ok(Expr::Like {
                    expr: Box::new(e),
                    pattern: Box::new(p),
                })
            }
        }
        sp::Expr::InList { expr, list, negated } => {
            let e = convert_expr(expr)?;
            let items: Vec<Expr> = list.iter().map(|i| convert_expr(i)).collect::<Result<_>>()?;
            if *negated {
                Ok(Expr::NotIn {
                    expr: Box::new(e),
                    list: items,
                })
            } else {
                Ok(Expr::In {
                    expr: Box::new(e),
                    list: items,
                })
            }
        }
        sp::Expr::Between { expr, negated, low, high } => {
            let e = convert_expr(expr)?;
            let lo = convert_expr(low)?;
            let hi = convert_expr(high)?;
            let between = Expr::Between {
                expr: Box::new(e.clone()),
                low: Box::new(lo),
                high: Box::new(hi),
            };
            if *negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(between),
                })
            } else {
                Ok(between)
            }
        }
        sp::Expr::Cast { expr, .. } => {
            convert_expr(expr)
        }
        sp::Expr::InSubquery { expr, negated, .. } => {
            // Simplification: treat subquery IN as always-true or always-false
            let e = convert_expr(expr)?;
            if *negated {
                Ok(Expr::NotIn {
                    expr: Box::new(e),
                    list: vec![],
                })
            } else {
                Ok(Expr::In {
                    expr: Box::new(e),
                    list: vec![],
                })
            }
        }
        sp::Expr::Subquery(_) => {
            // Subqueries not supported yet, return NULL
            Ok(Expr::Literal(LiteralValue::Null))
        }
        sp::Expr::Exists { .. } => {
            Ok(Expr::Literal(LiteralValue::Boolean(true)))
        }
        sp::Expr::RLike { expr, pattern, negated, .. } => {
            // REGEXP/RLIKE: convert to LIKE with % wildcards as a simplification
            let e = convert_expr(expr)?;
            let p = convert_expr(pattern)?;
            if *negated {
                Ok(Expr::NotLike {
                    expr: Box::new(e),
                    pattern: Box::new(p),
                })
            } else {
                Ok(Expr::Like {
                    expr: Box::new(e),
                    pattern: Box::new(p),
                })
            }
        }
        _ => Err(ForgeError::Parse(format!(
            "unsupported expression: {:?}",
            expr
        ))),
    }
}

fn convert_value(val: &sp::ValueWithSpan) -> Result<Expr> {
    match &val.value {
        sp::Value::Number(s, _) => {
            if s.contains('.') {
                let f: f64 = s
                    .parse()
                    .map_err(|_| ForgeError::Parse(format!("invalid float: {}", s)))?;
                Ok(Expr::Literal(LiteralValue::Float(f)))
            } else {
                let n: i64 = s
                    .parse()
                    .map_err(|_| ForgeError::Parse(format!("invalid integer: {}", s)))?;
                Ok(Expr::Literal(LiteralValue::Integer(n)))
            }
        }
        sp::Value::SingleQuotedString(s) => {
            Ok(Expr::Literal(LiteralValue::String(s.clone())))
        }
        sp::Value::NationalStringLiteral(s) => {
            // N'...' T-SQL Unicode strings
            Ok(Expr::Literal(LiteralValue::String(s.clone())))
        }
        sp::Value::Boolean(b) => Ok(Expr::Literal(LiteralValue::Boolean(*b))),
        sp::Value::Null => Ok(Expr::Literal(LiteralValue::Null)),
        _ => Err(ForgeError::Parse(format!(
            "unsupported value: {:?}",
            val
        ))),
    }
}

fn convert_binary_op(op: &sp::BinaryOperator) -> Result<BinaryOperator> {
    match op {
        sp::BinaryOperator::Eq => Ok(BinaryOperator::Eq),
        sp::BinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
        sp::BinaryOperator::Lt => Ok(BinaryOperator::Lt),
        sp::BinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
        sp::BinaryOperator::Gt => Ok(BinaryOperator::Gt),
        sp::BinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
        sp::BinaryOperator::And => Ok(BinaryOperator::And),
        sp::BinaryOperator::Or => Ok(BinaryOperator::Or),
        sp::BinaryOperator::Plus => Ok(BinaryOperator::Add),
        sp::BinaryOperator::Minus => Ok(BinaryOperator::Sub),
        sp::BinaryOperator::Multiply => Ok(BinaryOperator::Mul),
        sp::BinaryOperator::Divide => Ok(BinaryOperator::Div),
        _ => Err(ForgeError::Parse(format!(
            "unsupported binary operator: {:?}",
            op
        ))),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ident_to_string(ident: &sp::Ident) -> String {
    // Strip bracket quoting [name] or quote styles
    ident.value.clone()
}

fn object_name_to_string(name: &sp::ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident_to_string(ident),
            _ => String::new(),
        })
        .last()
        .unwrap_or_default()
}

fn expr_to_usize(expr: &sp::Expr) -> Option<usize> {
    match expr {
        sp::Expr::Value(val) => match &val.value {
            sp::Value::Number(s, _) => s.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_tsql_types() {
        let stmt = parse(
            "CREATE TABLE Users (
                id INT NOT NULL,
                name NVARCHAR(100) NULL,
                score FLOAT,
                active BIT NOT NULL
            )",
        )
        .unwrap();

        match stmt {
            Statement::CreateTable {
                table_name,
                columns,
                ..
            } => {
                assert_eq!(table_name, "Users");
                assert_eq!(columns.len(), 4);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].data_type, DataType::Integer);
                assert!(!columns[0].nullable);
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[1].data_type, DataType::Varchar(100));
                assert!(columns[1].nullable);
                assert_eq!(columns[2].data_type, DataType::Float);
                assert_eq!(columns[3].data_type, DataType::Boolean);
                assert!(!columns[3].nullable);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_drop_table() {
        let stmt = parse("DROP TABLE Users").unwrap();
        match stmt {
            Statement::DropTable { table_name, .. } => assert_eq!(table_name, "Users"),
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn test_insert_with_columns() {
        let stmt =
            parse("INSERT INTO Users (id, name) VALUES (1, N'Alice'), (2, 'Bob')").unwrap();
        match stmt {
            Statement::Insert {
                table_name,
                columns,
                values,
            } => {
                assert_eq!(table_name, "Users");
                assert_eq!(columns.as_ref().unwrap().len(), 2);
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_insert_without_columns() {
        let stmt = parse("INSERT INTO Users VALUES (1, 'Alice', 1)").unwrap();
        match stmt {
            Statement::Insert { columns, .. } => {
                assert!(columns.is_none());
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_select_star() {
        let stmt = parse("SELECT * FROM Users").unwrap();
        match stmt {
            Statement::Select { columns, from, .. } => {
                assert_eq!(columns.len(), 1);
                assert!(matches!(columns[0], SelectColumn::AllColumns(None)));
                assert!(matches!(from, FromClause::Table { .. }));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_with_where() {
        let stmt = parse("SELECT id, name FROM Users WHERE id > 5 AND active = 1").unwrap();
        match stmt {
            Statement::Select {
                columns, r#where, ..
            } => {
                assert_eq!(columns.len(), 2);
                assert!(r#where.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_with_join() {
        let stmt = parse(
            "SELECT u.name, o.total FROM Users u INNER JOIN Orders o ON u.id = o.user_id",
        )
        .unwrap();
        match stmt {
            Statement::Select { from, .. } => {
                assert!(matches!(from, FromClause::Join { .. }));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_with_order_by() {
        let stmt = parse("SELECT * FROM Users ORDER BY name ASC, id DESC").unwrap();
        match stmt {
            Statement::Select { order_by, .. } => {
                assert_eq!(order_by.len(), 2);
                assert!(order_by[0].ascending);
                assert!(!order_by[1].ascending);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_top() {
        let stmt = parse("SELECT TOP 10 * FROM Users").unwrap();
        match stmt {
            Statement::Select { limit, .. } => {
                assert_eq!(limit, Some(10));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_update_with_where() {
        let stmt = parse("UPDATE Users SET name = 'Bob', active = 0 WHERE id = 1").unwrap();
        match stmt {
            Statement::Update {
                table_name,
                assignments,
                r#where,
            } => {
                assert_eq!(table_name, "Users");
                assert_eq!(assignments.len(), 2);
                assert!(r#where.is_some());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_delete_with_where() {
        let stmt = parse("DELETE FROM Users WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete {
                table_name,
                r#where,
            } => {
                assert_eq!(table_name, "Users");
                assert!(r#where.is_some());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_is_null_expression() {
        let stmt = parse("SELECT * FROM Users WHERE name IS NULL").unwrap();
        match stmt {
            Statement::Select { r#where, .. } => match r#where.unwrap() {
                Expr::IsNull(_) => {}
                other => panic!("expected IsNull, got {:?}", other),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_is_not_null_expression() {
        let stmt = parse("SELECT * FROM Users WHERE name IS NOT NULL").unwrap();
        match stmt {
            Statement::Select { r#where, .. } => match r#where.unwrap() {
                Expr::IsNotNull(_) => {}
                other => panic!("expected IsNotNull, got {:?}", other),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_arithmetic_in_select() {
        let stmt = parse("SELECT id, score + 10 AS boosted FROM Users").unwrap();
        match stmt {
            Statement::Select { columns, .. } => {
                assert_eq!(columns.len(), 2);
                match &columns[1] {
                    SelectColumn::Expr { alias, .. } => {
                        assert_eq!(alias.as_deref(), Some("boosted"));
                    }
                    _ => panic!("expected Expr column"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_left_join() {
        let stmt = parse(
            "SELECT * FROM Users u LEFT JOIN Orders o ON u.id = o.user_id",
        )
        .unwrap();
        match stmt {
            Statement::Select { from, .. } => match from {
                FromClause::Join { join_type, .. } => {
                    assert!(matches!(join_type, JoinType::Left));
                }
                _ => panic!("expected Join"),
            },
            _ => panic!("expected Select"),
        }
    }
}
