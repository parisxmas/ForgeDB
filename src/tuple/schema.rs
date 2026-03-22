use crate::sql::ast::Expr;
use crate::tuple::types::{DataType, Value};

/// Describes a single column in a table schema.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub column_id: u16,
    pub auto_increment: bool,
    pub default_value: Option<Value>,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub check_expr: Option<Expr>,
    /// Foreign key reference: (parent_table, parent_column, on_delete_action)
    /// on_delete: 0=Restrict, 1=Cascade, 2=SetNull
    pub fk_ref: Option<(String, String, u8)>,
}

/// An ordered collection of [`Column`] definitions that describes the shape of
/// a tuple.
#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    /// Create a new schema from the given columns.
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    /// Look up a column by name (case-insensitive).
    ///
    /// Returns the column index and a reference to the [`Column`], or `None`
    /// if no column with the given name exists.
    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        let name_lower = name.to_lowercase();
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.to_lowercase() == name_lower)
    }

    /// Returns the number of columns in the schema.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Find the auto_increment column, returning its index and a reference.
    pub fn get_auto_increment_column(&self) -> Option<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.auto_increment)
    }

    /// Find the primary key column, returning its index and a reference.
    pub fn get_primary_key_column(&self) -> Option<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.is_primary_key)
    }
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Column {
                name: "Id".into(),
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
                name: "Name".into(),
                data_type: DataType::Varchar(255),
                nullable: true,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                is_unique: false,
                check_expr: None, fk_ref: None,
            },
            Column {
                name: "Active".into(),
                data_type: DataType::Boolean,
                nullable: false,
                column_id: 2,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                is_unique: false,
                check_expr: None, fk_ref: None,
            },
        ])
    }

    #[test]
    fn test_column_count() {
        assert_eq!(sample_schema().column_count(), 3);
    }

    #[test]
    fn test_get_column_case_insensitive() {
        let schema = sample_schema();
        let (idx, col) = schema.get_column("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.name, "Name");

        let (idx2, _) = schema.get_column("NAME").unwrap();
        assert_eq!(idx2, 1);
    }

    #[test]
    fn test_get_column_not_found() {
        assert!(sample_schema().get_column("nonexistent").is_none());
    }
}
