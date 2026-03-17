use crate::common::{PageId, RID};
use crate::error::Result;
use crate::sql::ast::{Expr, JoinType};
use crate::tuple::schema::{Column, Schema};
use crate::tuple::types::Value;

use super::eval::eval_to_bool;

/// Execute a nested-loop join between left and right row sets.
pub fn execute_nested_loop_join(
    left_rows: &[(RID, Vec<Value>)],
    right_rows: &[(RID, Vec<Value>)],
    join_type: &JoinType,
    on: &Expr,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    // Build combined schema
    let mut combined_columns = Vec::new();
    for col in &left_schema.columns {
        combined_columns.push(Column {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
            column_id: combined_columns.len() as u16,
            auto_increment: false,
            default_value: None,
            is_primary_key: false,
        });
    }
    for col in &right_schema.columns {
        combined_columns.push(Column {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: true, // joined columns may be null for outer joins
            column_id: combined_columns.len() as u16,
            auto_increment: false,
            default_value: None,
            is_primary_key: false,
        });
    }
    let combined_schema = Schema::new(combined_columns);

    let dummy_rid = RID {
        page_id: PageId(0),
        slot_id: 0,
    };
    let right_null_count = right_schema.columns.len();
    let left_null_count = left_schema.columns.len();

    let mut result = Vec::new();

    match join_type {
        JoinType::Inner => {
            for (_, lvals) in left_rows {
                for (_, rvals) in right_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    if eval_to_bool(on, &combined, &combined_schema)? {
                        result.push((dummy_rid, combined));
                    }
                }
            }
        }
        JoinType::Left => {
            for (_, lvals) in left_rows {
                let mut matched = false;
                for (_, rvals) in right_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    if eval_to_bool(on, &combined, &combined_schema)? {
                        result.push((dummy_rid, combined));
                        matched = true;
                    }
                }
                if !matched {
                    let mut combined = lvals.clone();
                    combined.extend(std::iter::repeat(Value::Null).take(right_null_count));
                    result.push((dummy_rid, combined));
                }
            }
        }
        JoinType::Right => {
            for (_, rvals) in right_rows {
                let mut matched = false;
                for (_, lvals) in left_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    if eval_to_bool(on, &combined, &combined_schema)? {
                        result.push((dummy_rid, combined));
                        matched = true;
                    }
                }
                if !matched {
                    let mut combined: Vec<Value> =
                        std::iter::repeat(Value::Null).take(left_null_count).collect();
                    combined.extend(rvals.iter().cloned());
                    result.push((dummy_rid, combined));
                }
            }
        }
    }

    Ok((combined_schema, result))
}
