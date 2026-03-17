use crate::common::RID;
use crate::error::Result;
use crate::sql::ast::Expr;
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

use super::eval::eval_to_bool;

/// Filter rows by a predicate expression.
pub fn execute_filter(
    predicate: &Expr,
    rows: Vec<(RID, Vec<Value>)>,
    schema: &Schema,
) -> Result<Vec<(RID, Vec<Value>)>> {
    let mut result = Vec::new();
    for (rid, values) in rows {
        if eval_to_bool(predicate, &values, schema)? {
            result.push((rid, values));
        }
    }
    Ok(result)
}
