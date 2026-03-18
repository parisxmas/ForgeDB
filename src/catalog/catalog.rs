use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Read};
use std::path::Path;

use crate::common::*;
use crate::error::{ForgeError, Result};
use crate::tuple::schema::{Column, Schema};
use crate::tuple::types::DataType;

/// Metadata about a single table stored in the catalog.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_id: TableId,
    pub name: String,
    pub schema: Schema,
    pub first_page_id: PageId,
}

/// In-memory catalog that tracks all tables in the database.
///
/// The catalog can be persisted to and loaded from a binary `.catalog` file so
/// that table definitions survive across restarts.
#[derive(Clone)]
pub struct Catalog {
    tables: HashMap<String, TableInfo>,
    next_table_id: u32,
    path: String,
}

impl Catalog {
    /// Create a new, empty catalog that will persist to the given file path.
    pub fn new(path: &str) -> Self {
        Self {
            tables: HashMap::new(),
            next_table_id: 0,
            path: path.to_string(),
        }
    }

    /// Register a new table. Returns the assigned [`TableId`].
    ///
    /// Returns an error if a table with the same name (case-insensitive)
    /// already exists.
    pub fn create_table(
        &mut self,
        name: &str,
        schema: Schema,
        first_page_id: PageId,
    ) -> Result<TableId> {
        let key = name.to_lowercase();
        if self.tables.contains_key(&key) {
            return Err(ForgeError::Catalog(format!(
                "table '{}' already exists",
                name
            )));
        }

        let table_id = TableId(self.next_table_id);
        self.next_table_id += 1;

        let info = TableInfo {
            table_id,
            name: name.to_string(),
            schema,
            first_page_id,
        };
        self.tables.insert(key, info);
        Ok(table_id)
    }

    /// Remove a table from the catalog. Returns the [`TableInfo`] that was
    /// removed.
    ///
    /// Returns an error if no table with the given name exists.
    pub fn drop_table(&mut self, name: &str) -> Result<TableInfo> {
        let key = name.to_lowercase();
        self.tables.remove(&key).ok_or_else(|| {
            ForgeError::Catalog(format!("table '{}' not found", name))
        })
    }

    /// Look up a table by name (case-insensitive).
    pub fn get_table(&self, name: &str) -> Option<&TableInfo> {
        let key = name.to_lowercase();
        self.tables.get(&key)
    }

    /// Look up a table by its [`TableId`] (linear scan).
    pub fn get_table_by_id(&self, table_id: TableId) -> Option<&TableInfo> {
        self.tables.values().find(|t| t.table_id == table_id)
    }

    /// Return references to all tables in the catalog (unordered).
    pub fn list_tables(&self) -> Vec<&TableInfo> {
        self.tables.values().collect()
    }

    // ------------------------------------------------------------------
    // Persistence
    // ------------------------------------------------------------------

    /// Serialize the catalog to its binary `.catalog` file.
    pub fn persist(&self) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();

        // Header
        let num_tables = self.tables.len() as u32;
        buf.extend_from_slice(&num_tables.to_le_bytes());
        buf.extend_from_slice(&self.next_table_id.to_le_bytes());

        for info in self.tables.values() {
            // table_id
            buf.extend_from_slice(&info.table_id.0.to_le_bytes());

            // name
            let name_bytes = info.name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);

            // first_page_id
            buf.extend_from_slice(&info.first_page_id.0.to_le_bytes());

            // columns
            let num_columns = info.schema.columns.len() as u32;
            buf.extend_from_slice(&num_columns.to_le_bytes());

            for col in &info.schema.columns {
                // col_name
                let col_name_bytes = col.name.as_bytes();
                buf.extend_from_slice(&(col_name_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(col_name_bytes);

                // data_type tag + optional payload
                match &col.data_type {
                    DataType::Integer => buf.push(0),
                    DataType::Float => buf.push(1),
                    DataType::Varchar(max_len) => {
                        buf.push(2);
                        buf.extend_from_slice(&max_len.to_le_bytes());
                    }
                    DataType::Boolean => buf.push(3),
                    DataType::BigInt => buf.push(4),
                    DataType::DateTime => buf.push(5),
                }

                // nullable
                buf.push(if col.nullable { 1 } else { 0 });

                // column_id
                buf.extend_from_slice(&col.column_id.to_le_bytes());
            }
        }

        fs::write(&self.path, &buf)?;
        Ok(())
    }

    /// Load a catalog from a binary `.catalog` file.
    ///
    /// If the file does not exist an empty catalog is returned.
    pub fn load(path: &str) -> Result<Self> {
        if !Path::new(path).exists() {
            return Ok(Self::new(path));
        }

        let data = fs::read(path)?;
        let mut cur = Cursor::new(&data);

        let num_tables = read_u32(&mut cur)?;
        let next_table_id = read_u32(&mut cur)?;

        let mut tables = HashMap::new();

        for _ in 0..num_tables {
            let table_id = TableId(read_u32(&mut cur)?);

            let name_len = read_u32(&mut cur)? as usize;
            let name = read_string(&mut cur, name_len)?;

            let first_page_id = PageId(read_u32(&mut cur)?);

            let num_columns = read_u32(&mut cur)?;
            let mut columns = Vec::with_capacity(num_columns as usize);

            for _ in 0..num_columns {
                let col_name_len = read_u32(&mut cur)? as usize;
                let col_name = read_string(&mut cur, col_name_len)?;

                let type_tag = read_u8(&mut cur)?;
                let data_type = match type_tag {
                    0 => DataType::Integer,
                    1 => DataType::Float,
                    2 => {
                        let max_len = read_u16(&mut cur)?;
                        DataType::Varchar(max_len)
                    }
                    3 => DataType::Boolean,
                    4 => DataType::BigInt,
                    5 => DataType::DateTime,
                    other => {
                        return Err(ForgeError::Catalog(format!(
                            "unknown data type tag: {}",
                            other
                        )));
                    }
                };

                let nullable = read_u8(&mut cur)? != 0;
                let column_id = read_u16(&mut cur)?;

                columns.push(Column {
                    name: col_name,
                    data_type,
                    nullable,
                    column_id,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                });
            }

            let key = name.to_lowercase();
            tables.insert(
                key,
                TableInfo {
                    table_id,
                    name,
                    schema: Schema::new(columns),
                    first_page_id,
                },
            );
        }

        Ok(Self {
            tables,
            next_table_id,
            path: path.to_string(),
        })
    }
}

// ------------------------------------------------------------------
// Binary read helpers
// ------------------------------------------------------------------

fn read_u8(cur: &mut Cursor<&Vec<u8>>) -> Result<u8> {
    let mut buf = [0u8; 1];
    cur.read_exact(&mut buf)
        .map_err(|e| ForgeError::Catalog(format!("unexpected EOF: {}", e)))?;
    Ok(buf[0])
}

fn read_u16(cur: &mut Cursor<&Vec<u8>>) -> Result<u16> {
    let mut buf = [0u8; 2];
    cur.read_exact(&mut buf)
        .map_err(|e| ForgeError::Catalog(format!("unexpected EOF: {}", e)))?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(cur: &mut Cursor<&Vec<u8>>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)
        .map_err(|e| ForgeError::Catalog(format!("unexpected EOF: {}", e)))?;
    Ok(u32::from_le_bytes(buf))
}

fn read_string(cur: &mut Cursor<&Vec<u8>>, len: usize) -> Result<String> {
    let mut buf = vec![0u8; len];
    cur.read_exact(&mut buf)
        .map_err(|e| ForgeError::Catalog(format!("unexpected EOF: {}", e)))?;
    String::from_utf8(buf).map_err(|e| ForgeError::Catalog(format!("invalid UTF-8: {}", e)))
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper: build a two-column schema (id INTEGER NOT NULL, name VARCHAR(100) NULLABLE).
    fn sample_schema() -> Schema {
        Schema::new(vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                column_id: 0,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Varchar(100),
                nullable: true,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
        ])
    }

    fn catalog_path(dir: &TempDir) -> String {
        dir.path().join("test.catalog").to_str().unwrap().to_string()
    }

    #[test]
    fn test_create_table() {
        let dir = TempDir::new().unwrap();
        let mut cat = Catalog::new(&catalog_path(&dir));

        let tid = cat.create_table("Users", sample_schema(), PageId(1)).unwrap();
        assert_eq!(tid, TableId(0));

        let info = cat.get_table("users").unwrap();
        assert_eq!(info.name, "Users");
        assert_eq!(info.first_page_id, PageId(1));
        assert_eq!(info.schema.column_count(), 2);
    }

    #[test]
    fn test_create_duplicate_table_errors() {
        let dir = TempDir::new().unwrap();
        let mut cat = Catalog::new(&catalog_path(&dir));

        cat.create_table("Users", sample_schema(), PageId(1)).unwrap();
        let result = cat.create_table("users", sample_schema(), PageId(2));
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_table() {
        let dir = TempDir::new().unwrap();
        let mut cat = Catalog::new(&catalog_path(&dir));

        cat.create_table("Users", sample_schema(), PageId(1)).unwrap();
        let info = cat.drop_table("USERS").unwrap();
        assert_eq!(info.name, "Users");
        assert!(cat.get_table("users").is_none());
    }

    #[test]
    fn test_drop_nonexistent_errors() {
        let dir = TempDir::new().unwrap();
        let mut cat = Catalog::new(&catalog_path(&dir));
        assert!(cat.drop_table("nope").is_err());
    }

    #[test]
    fn test_get_table_case_insensitive() {
        let dir = TempDir::new().unwrap();
        let mut cat = Catalog::new(&catalog_path(&dir));

        cat.create_table("Products", sample_schema(), PageId(5)).unwrap();
        assert!(cat.get_table("products").is_some());
        assert!(cat.get_table("PRODUCTS").is_some());
        assert!(cat.get_table("Products").is_some());
    }

    #[test]
    fn test_get_table_by_id() {
        let dir = TempDir::new().unwrap();
        let mut cat = Catalog::new(&catalog_path(&dir));

        let t0 = cat.create_table("A", sample_schema(), PageId(0)).unwrap();
        let t1 = cat.create_table("B", sample_schema(), PageId(1)).unwrap();

        assert_eq!(cat.get_table_by_id(t0).unwrap().name, "A");
        assert_eq!(cat.get_table_by_id(t1).unwrap().name, "B");
        assert!(cat.get_table_by_id(TableId(999)).is_none());
    }

    #[test]
    fn test_list_tables() {
        let dir = TempDir::new().unwrap();
        let mut cat = Catalog::new(&catalog_path(&dir));

        cat.create_table("A", sample_schema(), PageId(0)).unwrap();
        cat.create_table("B", sample_schema(), PageId(1)).unwrap();

        let mut names: Vec<&str> = cat.list_tables().iter().map(|t| t.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["A", "B"]);
    }

    #[test]
    fn test_persist_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = catalog_path(&dir);

        // Build a catalog with several tables covering all data types.
        {
            let mut cat = Catalog::new(&path);

            let schema1 = Schema::new(vec![
                Column {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    nullable: false,
                    column_id: 0,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                },
                Column {
                    name: "score".into(),
                    data_type: DataType::Float,
                    nullable: true,
                    column_id: 1,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                },
                Column {
                    name: "label".into(),
                    data_type: DataType::Varchar(200),
                    nullable: true,
                    column_id: 2,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                },
                Column {
                    name: "active".into(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    column_id: 3,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                },
            ]);

            let schema2 = Schema::new(vec![Column {
                name: "key".into(),
                data_type: DataType::Integer,
                nullable: false,
                column_id: 0,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            }]);

            cat.create_table("Items", schema1, PageId(10)).unwrap();
            cat.create_table("Keys", schema2, PageId(20)).unwrap();
            cat.persist().unwrap();
        }

        // Reload and verify.
        let cat = Catalog::load(&path).unwrap();
        assert_eq!(cat.list_tables().len(), 2);

        let items = cat.get_table("items").unwrap();
        assert_eq!(items.name, "Items");
        assert_eq!(items.table_id, TableId(0));
        assert_eq!(items.first_page_id, PageId(10));
        assert_eq!(items.schema.column_count(), 4);

        // Verify each column round-tripped correctly.
        let cols = &items.schema.columns;
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, DataType::Integer);
        assert!(!cols[0].nullable);
        assert_eq!(cols[0].column_id, 0);

        assert_eq!(cols[1].name, "score");
        assert_eq!(cols[1].data_type, DataType::Float);
        assert!(cols[1].nullable);

        assert_eq!(cols[2].name, "label");
        assert_eq!(cols[2].data_type, DataType::Varchar(200));

        assert_eq!(cols[3].name, "active");
        assert_eq!(cols[3].data_type, DataType::Boolean);
        assert!(!cols[3].nullable);

        let keys = cat.get_table("keys").unwrap();
        assert_eq!(keys.name, "Keys");
        assert_eq!(keys.table_id, TableId(1));
        assert_eq!(keys.first_page_id, PageId(20));
    }

    #[test]
    fn test_load_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.catalog");
        let cat = Catalog::load(path.to_str().unwrap()).unwrap();
        assert!(cat.list_tables().is_empty());
    }

    #[test]
    fn test_next_table_id_persists() {
        let dir = TempDir::new().unwrap();
        let path = catalog_path(&dir);

        {
            let mut cat = Catalog::new(&path);
            cat.create_table("A", sample_schema(), PageId(0)).unwrap();
            cat.create_table("B", sample_schema(), PageId(1)).unwrap();
            cat.persist().unwrap();
        }

        let mut cat = Catalog::load(&path).unwrap();
        // next_table_id should be 2, so the next table gets id 2.
        let tid = cat.create_table("C", sample_schema(), PageId(2)).unwrap();
        assert_eq!(tid, TableId(2));
    }
}
