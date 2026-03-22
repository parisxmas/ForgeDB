use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

use sqlparser::ast::{self as sp, ObjectNamePart, SelectItem, SetExpr, TableFactor, TopQuantity};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::error::{ForgeError, Result};
use crate::sql::ast::*;
use crate::tuple::types::DataType;

// =========================================================================
// Statement-level parse cache — avoids re-parsing identical SQL strings.
// Thread-safe via RwLock: concurrent readers don't block each other.
// Bounded at 50K entries; cleared entirely when full (simple eviction).
// =========================================================================

static PARSE_CACHE: std::sync::LazyLock<RwLock<HashMap<u64, Statement>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::with_capacity(1024)));

/// Hash SQL text using the standard hasher (fast for short strings).
fn hash_sql(sql: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

/// Parse a single SQL statement (Generic dialect for MySQL + T-SQL compat).
/// Uses a global parse cache to avoid re-parsing identical SQL strings.
pub fn parse(sql: &str) -> Result<Statement> {
    // Compute hash for cache lookup
    let hash = hash_sql(sql);

    // Check parse cache (read lock — concurrent readers don't block)
    if let Ok(cache) = PARSE_CACHE.read() {
        if let Some(cached) = cache.get(&hash) {
            return Ok(cached.clone());
        }
    }

    // Cache miss — parse normally
    let stmt = parse_uncached(sql)?;

    // Store in cache (write lock — brief)
    if let Ok(mut cache) = PARSE_CACHE.write() {
        if cache.len() >= 50_000 {
            cache.clear();
        }
        cache.insert(hash, stmt.clone());
    }

    Ok(stmt)
}

/// Clear the parse cache (e.g., on schema changes).
pub fn clear_parse_cache() {
    if let Ok(mut cache) = PARSE_CACHE.write() {
        cache.clear();
    }
}

/// Parse without cache — the actual parsing logic.
fn parse_uncached(sql: &str) -> Result<Statement> {
    // Fast-path: try lightweight INSERT parser to bypass sqlparser overhead
    if let Some(stmt) = try_fast_parse_insert(sql) {
        return Ok(stmt);
    }

    let dialect = MySqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ForgeError::Parse(format!("{}", e)))?;

    if statements.is_empty() {
        return Err(ForgeError::Parse("empty SQL".into()));
    }
    if statements.len() > 1 {
        return Err(ForgeError::Parse("multiple statements not supported".into()));
    }

    convert_statement(statements.into_iter().next().ok_or_else(|| ForgeError::Parse("no statements parsed".into()))?)
}

/// Fast-path INSERT parser for simple `INSERT INTO table VALUES (...)` statements.
/// Handles the common INSERT pattern without invoking the full sqlparser, which
/// saves ~0.3-0.5ms per statement. Returns None if the SQL doesn't match the
/// simple pattern, falling through to the full parser.
fn try_fast_parse_insert(sql: &str) -> Option<Statement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let bytes = trimmed.as_bytes();
    if bytes.len() < 20 {
        return None; // too short
    }

    // Check prefix case-insensitively: "INSERT INTO "
    let upper_start: String = trimmed.chars().take(12).collect::<String>().to_uppercase();
    if !upper_start.starts_with("INSERT INTO ") {
        return None;
    }

    let rest = &trimmed[12..];

    // Extract table name (stops at space, '(', or backtick)
    // Handle backtick-quoted table names
    let (table_name, after_table) = if rest.starts_with('`') {
        let end_tick = rest[1..].find('`')?;
        let name = &rest[1..1 + end_tick];
        (name, rest[2 + end_tick..].trim_start())
    } else {
        let end = rest.find(|c: char| c == ' ' || c == '(' || c == '\t' || c == '\n')?;
        (&rest[..end], rest[end..].trim_start())
    };

    if table_name.is_empty() {
        return None;
    }

    // Check for optional column list: (col1, col2, ...)
    let (columns, values_part) = if after_table.starts_with('(') {
        // Could be column list or VALUES — peek ahead
        // Find the matching closing paren
        let close = find_matching_paren(after_table)?;
        let inside = &after_table[1..close];
        let after_parens = after_table[close + 1..].trim_start();

        // Check if what follows starts with VALUES — if so, this paren group is a column list
        let after_upper: String = after_parens.chars().take(7).collect::<String>().to_uppercase();
        if after_upper.starts_with("VALUES") || after_upper.starts_with("VALUE") {
            // It's a column list
            let cols: Vec<String> = inside.split(',')
                .map(|c| c.trim().trim_matches('`').trim_matches('"').trim_matches('\'').to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if cols.is_empty() {
                return None;
            }
            (Some(cols), after_parens)
        } else {
            // No column list — the parens are part of VALUES
            (None, after_table)
        }
    } else {
        (None, after_table)
    };

    // Skip "VALUES" keyword
    let values_upper: String = values_part.chars().take(7).collect::<String>().to_uppercase();
    let after_values = if values_upper.starts_with("VALUES") {
        values_part[6..].trim_start()
    } else if values_upper.starts_with("VALUE") {
        values_part[5..].trim_start()
    } else {
        return None;
    };

    // Bail if there's ON DUPLICATE KEY or ON CONFLICT
    let after_upper_full = after_values.to_uppercase();
    if after_upper_full.contains("ON DUPLICATE") || after_upper_full.contains("ON CONFLICT") {
        return None;
    }

    // Parse one or more value tuples: (v1, v2, ...), (v1, v2, ...)
    let mut all_values = Vec::new();
    let mut remaining = after_values;

    loop {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }
        if !remaining.starts_with('(') {
            break;
        }

        let close = find_matching_paren(remaining)?;
        let inside = &remaining[1..close];
        let row_values = parse_value_list(inside)?;
        all_values.push(row_values);

        remaining = remaining[close + 1..].trim_start();
        if remaining.starts_with(',') {
            remaining = &remaining[1..];
        }
    }

    if all_values.is_empty() {
        return None;
    }

    Some(Statement::Insert {
        table_name: table_name.to_string(),
        columns,
        values: all_values,
        on_conflict: None,
    })
}

/// Find the index of the closing paren matching the opening paren at position 0.
/// Handles nested parens and string literals.
fn find_matching_paren(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'(' {
        return None;
    }
    let mut depth = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'\'' => {
                // Skip string literal
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2; // escaped quote
                            continue;
                        }
                        break;
                    }
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        i += 2; // backslash escape
                        continue;
                    }
                    i += 1;
                }
            }
            b'"' => {
                // Skip double-quoted string
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Parse a comma-separated list of SQL values into Expr literals.
/// Handles: integers, floats, strings ('...'), NULL, TRUE, FALSE.
fn parse_value_list(s: &str) -> Option<Vec<Expr>> {
    let mut values = Vec::new();
    let mut remaining = s.trim();

    while !remaining.is_empty() {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }

        let (expr, rest) = parse_single_value(remaining)?;
        values.push(expr);
        remaining = rest.trim_start();
        if remaining.starts_with(',') {
            remaining = &remaining[1..];
        }
    }

    if values.is_empty() {
        return None;
    }
    Some(values)
}

/// Parse a single SQL value and return (Expr, remaining_str).
fn parse_single_value(s: &str) -> Option<(Expr, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }

    // String literal
    if s.starts_with('\'') {
        let mut i = 1;
        let bytes = s.as_bytes();
        let mut result = String::new();
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    result.push('\'');
                    i += 2;
                    continue;
                }
                // End of string
                let rest = &s[i + 1..];
                return Some((Expr::Literal(LiteralValue::String(result)), rest));
            }
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                match bytes[i + 1] {
                    b'n' => result.push('\n'),
                    b't' => result.push('\t'),
                    b'\\' => result.push('\\'),
                    b'\'' => result.push('\''),
                    b'0' => result.push('\0'),
                    other => {
                        result.push('\\');
                        result.push(other as char);
                    }
                }
                i += 2;
                continue;
            }
            result.push(bytes[i] as char);
            i += 1;
        }
        return None; // unterminated string
    }

    // NULL
    if s.len() >= 4 {
        let upper4: String = s[..4].to_uppercase();
        if upper4 == "NULL" {
            let rest = &s[4..];
            if rest.is_empty() || rest.starts_with(',') || rest.starts_with(')') || rest.starts_with(' ') {
                return Some((Expr::Literal(LiteralValue::Null), rest));
            }
        }
    }

    // TRUE
    if s.len() >= 4 {
        let upper4: String = s[..4].to_uppercase();
        if upper4 == "TRUE" {
            let rest = &s[4..];
            if rest.is_empty() || rest.starts_with(',') || rest.starts_with(')') || rest.starts_with(' ') {
                return Some((Expr::Literal(LiteralValue::Boolean(true)), rest));
            }
        }
    }

    // FALSE
    if s.len() >= 5 {
        let upper5: String = s[..5].to_uppercase();
        if upper5 == "FALSE" {
            let rest = &s[5..];
            if rest.is_empty() || rest.starts_with(',') || rest.starts_with(')') || rest.starts_with(' ') {
                return Some((Expr::Literal(LiteralValue::Boolean(false)), rest));
            }
        }
    }

    // Negative number
    if s.starts_with('-') {
        let (inner, rest) = parse_number(&s[1..])?;
        match inner {
            Expr::Literal(LiteralValue::Integer(n)) => {
                return Some((Expr::Literal(LiteralValue::Integer(-n)), rest));
            }
            Expr::Literal(LiteralValue::Float(f)) => {
                return Some((Expr::Literal(LiteralValue::Float(-f)), rest));
            }
            _ => return None,
        }
    }

    // Positive number
    if s.as_bytes()[0].is_ascii_digit() {
        return parse_number(s);
    }

    // If we encounter something we can't handle (expressions, function calls, etc),
    // bail out to the full parser
    None
}

/// Parse a numeric literal (integer or float).
fn parse_number(s: &str) -> Option<(Expr, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut has_dot = false;

    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            i += 1;
        } else if bytes[i] == b'.' && !has_dot {
            has_dot = true;
            i += 1;
        } else if bytes[i] == b'e' || bytes[i] == b'E' {
            // Scientific notation
            i += 1;
            if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                i += 1;
            }
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            has_dot = true; // treat as float
            break;
        } else {
            break;
        }
    }

    if i == 0 {
        return None;
    }

    let num_str = &s[..i];
    let rest = &s[i..];

    if has_dot {
        let f: f64 = num_str.parse().ok()?;
        Some((Expr::Literal(LiteralValue::Float(f)), rest))
    } else {
        let n: i64 = num_str.parse().ok()?;
        Some((Expr::Literal(LiteralValue::Integer(n)), rest))
    }
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
        sp::Statement::Drop {
            object_type: sp::ObjectType::View,
            names,
            if_exists,
            ..
        } => {
            let name = object_name_to_string(&names[0]);
            Ok(Statement::DropView { name, if_exists })
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
        sp::Statement::Explain { statement, .. } => {
            let inner = convert_statement(*statement)?;
            Ok(Statement::Explain { statement: Box::new(inner) })
        }
        sp::Statement::Set(_) => {
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
        sp::Statement::CreateIndex(ci) => {
            let index_name = ci.name.as_ref()
                .map(|n| object_name_to_string(n))
                .unwrap_or_default();
            let table_name = object_name_to_string(&ci.table_name);
            let columns: Vec<String> = ci.columns.iter()
                .map(|c| c.column.expr.to_string())
                .collect();
            let include_columns: Vec<String> = ci.include.iter()
                .map(|ident| ident_to_string(ident))
                .collect();
            Ok(Statement::CreateIndex {
                index_name,
                table_name,
                columns,
                unique: ci.unique,
                include_columns,
            })
        }
        sp::Statement::CreateView(cv) => {
            let name = object_name_to_string(&cv.name);
            let column_aliases = if cv.columns.is_empty() {
                None
            } else {
                Some(cv.columns.iter().map(|c| ident_to_string(&c.name)).collect())
            };
            let query = convert_query(*cv.query)?;
            Ok(Statement::CreateView {
                name,
                column_aliases,
                query: Box::new(query),
            })
        }
        sp::Statement::AlterTable(at) => {
            let table_name = object_name_to_string(&at.name);
            let mut ops = Vec::new();
            for op in at.operations {
                match op {
                    sp::AlterTableOperation::AddColumn { column_def, .. } => {
                        let col = convert_column_def(&column_def)?;
                        ops.push(AlterTableOp::AddColumn(col));
                    }
                    sp::AlterTableOperation::DropColumn { column_names, .. } => {
                        for cn in &column_names {
                            ops.push(AlterTableOp::DropColumn(ident_to_string(cn)));
                        }
                    }
                    sp::AlterTableOperation::RenameColumn { old_column_name, new_column_name, .. } => {
                        ops.push(AlterTableOp::RenameColumn {
                            old_name: ident_to_string(&old_column_name),
                            new_name: ident_to_string(&new_column_name),
                        });
                    }
                    _ => {}
                }
            }
            Ok(Statement::AlterTable { table_name, operations: ops })
        }
        sp::Statement::Analyze(analyze) => {
            let table_name = analyze.table_name
                .as_ref()
                .map(|n| object_name_to_string(n))
                .unwrap_or_default();
            Ok(Statement::AnalyzeTable { table_name })
        }
        sp::Statement::Truncate(truncate) => {
            if truncate.table_names.is_empty() {
                return Err(ForgeError::Parse("TRUNCATE without table name".into()));
            }
            let name = object_name_to_string(&truncate.table_names[0].name);
            Ok(Statement::TruncateTable { table_name: name })
        }
        sp::Statement::Savepoint { name } => {
            Ok(Statement::Savepoint { name: ident_to_string(&name) })
        }
        sp::Statement::ReleaseSavepoint { name } => {
            Ok(Statement::ReleaseSavepoint { name: ident_to_string(&name) })
        }
        sp::Statement::Rollback { savepoint, .. } => {
            if let Some(sp_name) = savepoint {
                Ok(Statement::RollbackTo { name: ident_to_string(&sp_name) })
            } else {
                Ok(Statement::Rollback)
            }
        }
        sp::Statement::CreateSequence { name: ref seq_name, ref sequence_options, .. } => {
            let name = object_name_to_string(seq_name);
            let mut start = 1i64;
            let mut increment = 1i64;
            for opt in sequence_options {
                match opt {
                    sp::SequenceOptions::StartWith(v, _) => {
                        if let sp::Expr::Value(val) = v {
                            if let sp::Value::Number(s, _) = &val.value {
                                start = s.parse().unwrap_or(1);
                            }
                        }
                    }
                    sp::SequenceOptions::IncrementBy(v, _) => {
                        if let sp::Expr::Value(val) = v {
                            if let sp::Value::Number(s, _) = &val.value {
                                increment = s.parse().unwrap_or(1);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(Statement::CreateSequence { name, start, increment })
        }
        sp::Statement::CreateDatabase { db_name, .. } => {
            let name = object_name_to_string(&db_name);
            Ok(Statement::CreateDatabase { name })
        }
        sp::Statement::Drop {
            object_type: sp::ObjectType::Database,
            names,
            ..
        } => {
            let name = object_name_to_string(&names[0]);
            Ok(Statement::DropDatabase { name })
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
        columns.push(convert_column_def(col)?);
    }

    // Handle table-level PRIMARY KEY constraints
    for constraint in &ct.constraints {
        match constraint {
            sp::TableConstraint::PrimaryKey(pk) => {
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
            sp::TableConstraint::Unique(uniq) => {
                for idx_col in &uniq.columns {
                    let col_name = idx_col.column.expr.to_string();
                    for col in &mut columns {
                        if col.name.eq_ignore_ascii_case(&col_name) {
                            col.is_unique = true;
                        }
                    }
                }
            }
            sp::TableConstraint::ForeignKey(fk) => {
                let ref_table_name = object_name_to_string(&fk.foreign_table);
                let on_delete = match &fk.on_delete {
                    Some(sp::ReferentialAction::Cascade) => FkAction::Cascade,
                    Some(sp::ReferentialAction::SetNull) => FkAction::SetNull,
                    _ => FkAction::Restrict,
                };
                for (i, idx_col) in fk.columns.iter().enumerate() {
                    let col_name = ident_to_string(idx_col);
                    let ref_col = fk.referred_columns.get(i)
                        .map(|c| ident_to_string(c))
                        .unwrap_or_default();
                    for col in &mut columns {
                        if col.name.eq_ignore_ascii_case(&col_name) {
                            col.references = Some(ForeignKeyRef {
                                table: ref_table_name.clone(),
                                column: ref_col.clone(),
                                on_delete: on_delete.clone(),
                            });
                        }
                    }
                }
            }
            sp::TableConstraint::Check(check) => {
                if let Ok(check_expr) = convert_expr(&check.expr) {
                    if let Some(last) = columns.last_mut() {
                        if last.check_expr.is_none() {
                            last.check_expr = Some(check_expr);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Statement::CreateTable {
        table_name,
        columns,
        if_not_exists: ct.if_not_exists,
    })
}

fn convert_column_def(col: &sp::ColumnDef) -> Result<ColumnDef> {
    let name = ident_to_string(&col.name);
    let data_type = convert_data_type(&col.data_type)?;

    let mut nullable = true;
    let mut auto_increment = false;
    let mut default_value = None;
    let mut is_primary_key = false;
    let mut is_unique = false;
    let mut check_expr = None;
    let mut references = None;

    for opt in &col.options {
        match &opt.option {
            sp::ColumnOption::NotNull => nullable = false,
            sp::ColumnOption::Null => nullable = true,
            sp::ColumnOption::Default(expr) => {
                default_value = convert_expr(expr).ok().map(|e| Some(e)).unwrap_or(None);
            }
            sp::ColumnOption::Unique(uc) => {
                let _ = uc;
                is_unique = true;
            }
            sp::ColumnOption::PrimaryKey(_) => {
                is_primary_key = true;
                nullable = false;
            }
            sp::ColumnOption::ForeignKey(fk) => {
                let ref_table = object_name_to_string(&fk.foreign_table);
                let ref_col = fk.referred_columns.first()
                    .map(|c| ident_to_string(c))
                    .unwrap_or_default();
                let on_delete = match &fk.on_delete {
                    Some(sp::ReferentialAction::Cascade) => FkAction::Cascade,
                    Some(sp::ReferentialAction::SetNull) => FkAction::SetNull,
                    _ => FkAction::Restrict,
                };
                references = Some(ForeignKeyRef { table: ref_table, column: ref_col, on_delete });
            }
            sp::ColumnOption::Check(check_c) => {
                check_expr = convert_expr(&check_c.expr).ok();
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

    Ok(ColumnDef {
        name,
        data_type,
        nullable,
        auto_increment,
        default_value,
        is_primary_key,
        is_unique,
        check_expr,
        references,
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
        sp::DataType::Decimal(info) | sp::DataType::Dec(info) | sp::DataType::Numeric(info) => {
            let (prec, scale) = extract_decimal_info(info);
            Ok(DataType::Decimal(prec, scale))
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
        sp::DataType::Date => Ok(DataType::Date),
        sp::DataType::Time(_, _) => Ok(DataType::Time),
        sp::DataType::Varbinary(len_info) => {
            let n = len_info.as_ref().and_then(|bl| match bl {
                sp::BinaryLength::IntegerLength { length, .. } => Some(*length as u16),
                sp::BinaryLength::Max => Some(u16::MAX),
            }).unwrap_or(255);
            Ok(DataType::VarBinary(n))
        }
        sp::DataType::Binary(len_info) => {
            let n = len_info.unwrap_or(255) as u16;
            Ok(DataType::VarBinary(n))
        }
        sp::DataType::JSON => Ok(DataType::Json),
        sp::DataType::Uuid => Ok(DataType::Uuid),
        _ => Err(ForgeError::Parse(format!(
            "unsupported data type: {:?}",
            dt
        ))),
    }
}

fn convert_sp_data_type_to_ours(dt: &sp::DataType) -> DataType {
    convert_data_type(dt).unwrap_or(DataType::Varchar(255))
}

fn extract_decimal_info(info: &sp::ExactNumberInfo) -> (u8, u8) {
    match info {
        sp::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
        sp::ExactNumberInfo::Precision(p) => (*p as u8, 0),
        sp::ExactNumberInfo::None => (18, 0),
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

    // Parse ON DUPLICATE KEY UPDATE
    let on_conflict = if let Some(ref on_dup) = ins.on {
        match on_dup {
            sp::OnInsert::DuplicateKeyUpdate(assignments) => {
                let mut our_assignments = Vec::new();
                for a in assignments {
                    let col_name = match &a.target {
                        sp::AssignmentTarget::ColumnName(names) => {
                            names.0.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(".")
                        }
                        _ => continue,
                    };
                    let val = convert_expr(&a.value)?;
                    our_assignments.push(Assignment { column: col_name, value: val });
                }
                Some(OnConflict {
                    columns: vec![],
                    action: OnConflictAction::DoUpdate(our_assignments),
                })
            }
            _ => None,
        }
    } else {
        None
    };

    match source.body.as_ref() {
        SetExpr::Values(vals) => {
            let mut rows = Vec::new();
            for row in &vals.rows {
                let mut exprs = Vec::new();
                for expr in row {
                    exprs.push(convert_expr(expr)?);
                }
                rows.push(exprs);
            }
            Ok(Statement::Insert {
                table_name,
                columns,
                values: rows,
                on_conflict,
            })
        }
        SetExpr::Select(_) | SetExpr::SetOperation { .. } => {
            // INSERT INTO ... SELECT ...
            let query = convert_query(*ins.source.ok_or_else(|| ForgeError::Parse("INSERT ... SELECT missing source query".into()))?.clone())?;
            Ok(Statement::InsertSelect {
                table_name,
                columns,
                query: Box::new(query),
            })
        }
        _ => Err(ForgeError::Parse("expected VALUES or SELECT in INSERT".into())),
    }
}

// ---------------------------------------------------------------------------
// SELECT / Query
// ---------------------------------------------------------------------------

fn convert_query(query: sp::Query) -> Result<Statement> {
    // CTEs (WITH ... AS ...)
    let ctes = if let Some(ref with) = query.with {
        let is_recursive = with.recursive;
        let mut result = Vec::new();
        for cte_item in &with.cte_tables {
            let name = ident_to_string(&cte_item.alias.name);
            let cte_query = convert_query(*cte_item.query.clone())?;
            result.push(Cte { name, query: Box::new(cte_query), recursive: is_recursive });
        }
        result
    } else {
        vec![]
    };

    // ORDER BY
    let order_by = if let Some(ref ob) = query.order_by {
        convert_order_by(ob)?
    } else {
        vec![]
    };

    // LIMIT (standard SQL)
    let (limit_val, offset_val) = if let Some(ref lc) = query.limit_clause {
        match lc {
            sp::LimitClause::LimitOffset { limit, offset, .. } => {
                let l = limit.as_ref().and_then(|e| expr_to_usize(e));
                let o = offset.as_ref().and_then(|o| expr_to_usize(&o.value));
                (l, o)
            }
            _ => (None, None),
        }
    } else {
        (None, None)
    };

    // FETCH FIRST / NEXT n ROWS ONLY (standard SQL OFFSET-FETCH)
    let (limit_val, offset_val) = if let Some(ref fetch) = query.fetch {
        let fetch_count = fetch.quantity.as_ref().and_then(|e| expr_to_usize(e));
        (limit_val.or(fetch_count), offset_val)
    } else {
        (limit_val, offset_val)
    };

    // Handle UNION / UNION ALL
    if matches!(query.body.as_ref(), SetExpr::SetOperation { .. }) {
        let body = *query.body;
        if let SetExpr::SetOperation { op, left, right, set_quantifier, .. } = body {
            match op {
                sp::SetOperator::Union => {
                    let all = matches!(set_quantifier, sp::SetQuantifier::All);
                    let left_stmt = convert_set_expr_to_stmt(*left)?;
                    let right_stmt = convert_set_expr_to_stmt(*right)?;
                    return Ok(Statement::Union {
                        left: Box::new(left_stmt),
                        right: Box::new(right_stmt),
                        all,
                    });
                }
                _ => return Err(ForgeError::Parse(format!("unsupported set operation: {:?}", op)))
            }
        }
        unreachable!()
    }

    match *query.body {
        SetExpr::Select(sel) => {
            convert_select_with_ctes(*sel, order_by, limit_val, offset_val, ctes)
        }
        _ => Err(ForgeError::Parse("expected SELECT or set operation".into())),
    }
}

fn convert_set_expr_to_stmt(expr: SetExpr) -> Result<Statement> {
    match expr {
        SetExpr::Select(sel) => convert_select(*sel, vec![], None, None),
        SetExpr::SetOperation { op, left, right, set_quantifier, .. } => {
            if matches!(op, sp::SetOperator::Union) {
                let all = matches!(set_quantifier, sp::SetQuantifier::All);
                let left_stmt = convert_set_expr_to_stmt(*left)?;
                let right_stmt = convert_set_expr_to_stmt(*right)?;
                Ok(Statement::Union {
                    left: Box::new(left_stmt),
                    right: Box::new(right_stmt),
                    all,
                })
            } else {
                Err(ForgeError::Parse(format!("unsupported set operation: {:?}", op)))
            }
        }
        _ => Err(ForgeError::Parse("expected SELECT in set operation".into())),
    }
}

fn convert_select(
    select: sp::Select,
    order_by: Vec<OrderByItem>,
    limit_val: Option<usize>,
    offset_val: Option<usize>,
) -> Result<Statement> {
    convert_select_with_ctes(select, order_by, limit_val, offset_val, vec![])
}

fn convert_select_with_ctes(
    select: sp::Select,
    order_by: Vec<OrderByItem>,
    limit_val: Option<usize>,
    offset_val: Option<usize>,
    ctes: Vec<Cte>,
) -> Result<Statement> {
    // DISTINCT
    let distinct = select.distinct.is_some();

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
        // Support SELECT without FROM (e.g., SELECT 1+1)
        FromClause::Table { name: "__dual__".to_string(), alias: None }
    } else {
        convert_from(&select.from)?
    };

    // WHERE
    let where_clause = select
        .selection
        .as_ref()
        .map(|e| convert_expr(e))
        .transpose()?;

    // GROUP BY
    let group_by = match &select.group_by {
        sp::GroupByExpr::Expressions(exprs, _) => {
            exprs.iter().map(|e| convert_expr(e)).collect::<Result<Vec<_>>>()?
        }
        sp::GroupByExpr::All(_) => vec![],
    };

    // HAVING
    let having = select
        .having
        .as_ref()
        .map(|e| convert_expr(e))
        .transpose()?;

    Ok(Statement::Select {
        distinct,
        columns,
        from,
        r#where: where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset: offset_val,
        ctes,
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

    // Handle implicit cross join: FROM t1, t2
    for extra in from.iter().skip(1) {
        let right = convert_table_factor(&extra.relation)?;
        result = FromClause::Join {
            left: Box::new(result),
            right: Box::new(right),
            join_type: JoinType::Cross,
            on: None,
        };
        for join in &extra.joins {
            let right = convert_table_factor(&join.relation)?;
            let (join_type, on_expr) = convert_join_operator(&join.join_operator)?;
            result = FromClause::Join {
                left: Box::new(result),
                right: Box::new(right),
                join_type,
                on: on_expr,
            };
        }
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
        TableFactor::Derived { subquery, alias, .. } => {
            let query = convert_query(*subquery.clone())?;
            let alias_name = alias.as_ref()
                .map(|a| ident_to_string(&a.name))
                .unwrap_or_else(|| "subquery".to_string());
            Ok(FromClause::Subquery {
                query: Box::new(query),
                alias: alias_name,
            })
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            let mut result = convert_table_factor(&table_with_joins.relation)?;
            for join in &table_with_joins.joins {
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
        _ => Err(ForgeError::Parse("unsupported FROM clause element".into())),
    }
}

fn convert_join_operator(op: &sp::JoinOperator) -> Result<(JoinType, Option<Expr>)> {
    match op {
        sp::JoinOperator::Inner(c) | sp::JoinOperator::Join(c) => {
            let on_expr = extract_join_constraint(c)?;
            Ok((JoinType::Inner, Some(on_expr)))
        }
        sp::JoinOperator::Left(c) | sp::JoinOperator::LeftOuter(c) => {
            let on_expr = extract_join_constraint(c)?;
            Ok((JoinType::Left, Some(on_expr)))
        }
        sp::JoinOperator::Right(c) | sp::JoinOperator::RightOuter(c) => {
            let on_expr = extract_join_constraint(c)?;
            Ok((JoinType::Right, Some(on_expr)))
        }
        sp::JoinOperator::FullOuter(c, ..) => {
            let on_expr = extract_join_constraint(c)?;
            Ok((JoinType::Full, Some(on_expr)))
        }
        sp::JoinOperator::CrossJoin(_) => {
            Ok((JoinType::Cross, None))
        }
        _ => Err(ForgeError::Parse("unsupported join type".into())),
    }
}

fn extract_join_constraint(constraint: &sp::JoinConstraint) -> Result<Expr> {
    match constraint {
        sp::JoinConstraint::On(expr) => convert_expr(expr),
        _ => Err(ForgeError::Parse("only ON join constraint supported".into())),
    }
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
                let col = ident_to_string(parts.last().ok_or_else(|| ForgeError::Parse("empty compound identifier".into()))?);
                let table = ident_to_string(&parts[parts.len() - 2]);
                Ok(Expr::ColumnRef {
                    table: Some(table),
                    column: col,
                })
            }
        }
        sp::Expr::Value(val) => convert_value(val),
        sp::Expr::BinaryOp { left, op, right } => {
            // Handle string concatenation ||
            if matches!(op, sp::BinaryOperator::StringConcat) {
                return Ok(Expr::Concat {
                    left: Box::new(convert_expr(left)?),
                    right: Box::new(convert_expr(right)?),
                });
            }
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
            let mut fn_distinct = false;
            match &func.args {
                sp::FunctionArguments::List(arg_list) => {
                    for dup in &arg_list.duplicate_treatment {
                        if matches!(dup, sp::DuplicateTreatment::Distinct) {
                            fn_distinct = true;
                        }
                    }
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
            // Check for OVER clause (window function)
            if let Some(ref window_type) = func.over {
                match window_type {
                    sp::WindowType::WindowSpec(spec) => {
                        let partition_by = spec.partition_by.iter()
                            .map(|e| convert_expr(e))
                            .collect::<Result<Vec<_>>>()?;
                        let order_by = if spec.order_by.is_empty() {
                            vec![]
                        } else {
                            let mut items = Vec::new();
                            for obe in &spec.order_by {
                                let expr = convert_expr(&obe.expr)?;
                                let ascending = obe.options.asc.unwrap_or(true);
                                items.push(OrderByItem { expr, ascending });
                            }
                            items
                        };
                        return Ok(Expr::WindowFunction {
                            name,
                            args,
                            partition_by,
                            order_by,
                        });
                    }
                    _ => {}
                }
            }
            Ok(Expr::Function { name, args, distinct: fn_distinct })
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
        sp::Expr::Case { operand, conditions, else_result, .. } => {
            let op = operand.as_ref().map(|e| convert_expr(e)).transpose()?;
            let mut when_clauses = Vec::new();
            for cw in conditions {
                when_clauses.push((convert_expr(&cw.condition)?, convert_expr(&cw.result)?));
            }
            let else_r = else_result.as_ref().map(|e| convert_expr(e)).transpose()?;
            Ok(Expr::Case {
                operand: op.map(Box::new),
                when_clauses,
                else_result: else_r.map(Box::new),
            })
        }
        sp::Expr::Cast { expr, data_type, .. } => {
            let e = convert_expr(expr)?;
            let dt = convert_sp_data_type_to_ours(data_type);
            Ok(Expr::Cast { expr: Box::new(e), data_type: dt })
        }
        sp::Expr::InSubquery { expr, subquery, negated } => {
            let e = convert_expr(expr)?;
            let sub = convert_query(*subquery.clone())?;
            Ok(Expr::InSubquery {
                expr: Box::new(e),
                subquery: Box::new(sub),
                negated: *negated,
            })
        }
        sp::Expr::Subquery(query) => {
            let sub = convert_query(*query.clone())?;
            Ok(Expr::Subquery(Box::new(sub)))
        }
        sp::Expr::Exists { subquery, negated } => {
            let sub = convert_query(*subquery.clone())?;
            Ok(Expr::Exists {
                subquery: Box::new(sub),
                negated: *negated,
            })
        }
        sp::Expr::RLike { expr, pattern, negated, .. } => {
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
        // SUBSTRING(expr, from, for) - special parsed expression
        sp::Expr::Substring { expr, substring_from, substring_for, .. } => {
            let mut args = vec![convert_expr(expr)?];
            if let Some(from) = substring_from {
                args.push(convert_expr(from)?);
            }
            if let Some(for_len) = substring_for {
                args.push(convert_expr(for_len)?);
            }
            Ok(Expr::Function { name: "SUBSTRING".to_string(), args, distinct: false })
        }
        // TRIM(expr) - special parsed expression
        sp::Expr::Trim { expr, trim_what, .. } => {
            let mut args = vec![convert_expr(expr)?];
            if let Some(what) = trim_what {
                args.push(convert_expr(what)?);
            }
            Ok(Expr::Function { name: "TRIM".to_string(), args, distinct: false })
        }
        // CEIL(expr) / FLOOR(expr) - special parsed expressions
        sp::Expr::Ceil { expr, .. } => {
            Ok(Expr::Function { name: "CEIL".to_string(), args: vec![convert_expr(expr)?], distinct: false })
        }
        sp::Expr::Floor { expr, .. } => {
            Ok(Expr::Function { name: "FLOOR".to_string(), args: vec![convert_expr(expr)?], distinct: false })
        }
        // POSITION(expr IN expr)
        sp::Expr::Position { expr, r#in, .. } => {
            Ok(Expr::Function {
                name: "POSITION".to_string(),
                args: vec![convert_expr(expr)?, convert_expr(r#in)?],
                distinct: false,
            })
        }
        // EXTRACT(field FROM expr)
        sp::Expr::Extract { field, expr, .. } => {
            let field_name = format!("{}", field);
            Ok(Expr::Function {
                name: "EXTRACT".to_string(),
                args: vec![Expr::Literal(LiteralValue::String(field_name)), convert_expr(expr)?],
                distinct: false,
            })
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
        sp::BinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
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
                ..
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

    #[test]
    fn test_group_by() {
        let stmt = parse("SELECT dept, COUNT(*) FROM Employees GROUP BY dept").unwrap();
        match stmt {
            Statement::Select { group_by, .. } => {
                assert_eq!(group_by.len(), 1);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_distinct() {
        let stmt = parse("SELECT DISTINCT name FROM Users").unwrap();
        match stmt {
            Statement::Select { distinct, .. } => {
                assert!(distinct);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_case_expression() {
        let stmt = parse("SELECT CASE WHEN id > 5 THEN 'big' ELSE 'small' END FROM Users").unwrap();
        match stmt {
            Statement::Select { columns, .. } => {
                match &columns[0] {
                    SelectColumn::Expr { expr, .. } => {
                        assert!(matches!(expr, Expr::Case { .. }));
                    }
                    _ => panic!("expected Case expr"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_union() {
        let stmt = parse("SELECT id FROM Users UNION ALL SELECT id FROM Admins").unwrap();
        assert!(matches!(stmt, Statement::Union { all: true, .. }));
    }
}
