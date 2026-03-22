//! Temporary file storage for spill-to-disk operations.
//!
//! Provides RAII-managed temp files for external sort and grace hash join.
//! Each `TempFile` deletes its backing file on drop.

use std::fs;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::tuple::types::Value;

/// A temporary file that is deleted when dropped.
pub struct TempFile {
    path: PathBuf,
}

impl TempFile {
    /// Create a new temp file at the given path.
    pub fn new(path: PathBuf) -> io::Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self { path })
    }

    /// Get the path to this temp file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open a buffered writer to this temp file.
    pub fn writer(&self) -> io::Result<BufWriter<fs::File>> {
        let f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        Ok(BufWriter::with_capacity(65536, f))
    }

    /// Open a buffered reader for this temp file.
    pub fn reader(&self) -> io::Result<BufReader<fs::File>> {
        let f = fs::File::open(&self.path)?;
        Ok(BufReader::with_capacity(65536, f))
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Manages a collection of temp files. All files are cleaned up on drop.
pub struct TempFileManager {
    dir: PathBuf,
    files: Vec<TempFile>,
    counter: u64,
}

impl TempFileManager {
    /// Create a new manager that places temp files in the given directory.
    pub fn new() -> io::Result<Self> {
        let dir = std::env::temp_dir().join(format!("forgedb_temp_{}", std::process::id()));
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            files: Vec::new(),
            counter: 0,
        })
    }

    /// Allocate a new temp file and return a reference to it.
    pub fn create_file(&mut self, prefix: &str) -> io::Result<&TempFile> {
        self.counter += 1;
        let path = self.dir.join(format!("{}_{}.tmp", prefix, self.counter));
        let tf = TempFile::new(path)?;
        self.files.push(tf);
        Ok(self.files.last().ok_or_else(|| io::Error::new(io::ErrorKind::Other, "temp file list unexpectedly empty"))?)
    }
}

impl Drop for TempFileManager {
    fn drop(&mut self) {
        self.files.clear(); // TempFile::drop cleans each file
        let _ = fs::remove_dir(&self.dir);
    }
}

// ---------------------------------------------------------------------------
// Row serialization for temp files
// ---------------------------------------------------------------------------
// Format per row: [total_len: u32][value_count: u16][value_type: u8 + data]...

/// Serialize a row of Values to a byte vector.
pub fn serialize_row(row: &[Value]) -> Vec<u8> {
    let mut data = Vec::with_capacity(64);
    // Placeholder for total_len (will fill in at end)
    data.extend_from_slice(&0u32.to_le_bytes());
    // value count
    data.extend_from_slice(&(row.len() as u16).to_le_bytes());

    for val in row {
        match val {
            Value::Null => {
                data.push(0);
            }
            Value::Integer(n) => {
                data.push(1);
                data.extend_from_slice(&n.to_le_bytes());
            }
            Value::BigInt(n) => {
                data.push(2);
                data.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float(f) => {
                data.push(3);
                data.extend_from_slice(&f.to_le_bytes());
            }
            Value::Varchar(s) => {
                data.push(4);
                let bytes = s.as_bytes();
                data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(bytes);
            }
            Value::Boolean(b) => {
                data.push(5);
                data.push(if *b { 1 } else { 0 });
            }
            Value::DateTime(t) => {
                data.push(6);
                data.extend_from_slice(&t.to_le_bytes());
            }
            Value::Decimal(v, scale) => {
                data.push(7);
                data.extend_from_slice(&v.to_le_bytes());
                data.push(*scale);
            }
            Value::Date(d) => {
                data.push(8);
                data.extend_from_slice(&d.to_le_bytes());
            }
            Value::Time(t) => {
                data.push(9);
                data.extend_from_slice(&t.to_le_bytes());
            }
            Value::Binary(b) => {
                data.push(10);
                data.extend_from_slice(&(b.len() as u32).to_le_bytes());
                data.extend_from_slice(b);
            }
            Value::Json(s) => {
                data.push(11);
                let bytes = s.as_bytes();
                data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(bytes);
            }
            Value::Uuid(s) => {
                data.push(12);
                let bytes = s.as_bytes();
                data.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                data.extend_from_slice(bytes);
            }
        }
    }

    // Fill in total_len (not including the 4-byte len field itself)
    let total_len = (data.len() - 4) as u32;
    data[0..4].copy_from_slice(&total_len.to_le_bytes());
    data
}

/// Deserialize a row of Values from a reader. Returns None on EOF.
pub fn deserialize_row(reader: &mut impl Read) -> io::Result<Option<Vec<Value>>> {
    // Read total_len
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let total_len = u32::from_le_bytes(len_buf) as usize;

    // Read the rest of the row
    let mut data = vec![0u8; total_len];
    reader.read_exact(&mut data)?;

    let value_count = u16::from_le_bytes([data[0], data[1]]) as usize;
    let mut offset = 2;
    let mut values = Vec::with_capacity(value_count);

    for _ in 0..value_count {
        if offset >= data.len() {
            break;
        }
        let type_tag = data[offset];
        offset += 1;
        let val = match type_tag {
            0 => Value::Null,
            1 => {
                let n = i32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Integer(n)
            }
            2 => {
                let n = i64::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::BigInt(n)
            }
            3 => {
                let f = f64::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Float(f)
            }
            4 => {
                let str_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;
                let s = String::from_utf8_lossy(&data[offset..offset + str_len]).to_string();
                offset += str_len;
                Value::Varchar(s)
            }
            5 => {
                let b = data[offset] != 0;
                offset += 1;
                Value::Boolean(b)
            }
            6 => {
                let t = i64::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::DateTime(t)
            }
            7 => {
                // Decimal
                let v = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let scale = data[offset];
                offset += 1;
                Value::Decimal(v, scale)
            }
            8 => {
                // Date
                let d = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;
                Value::Date(d)
            }
            9 => {
                // Time
                let t = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;
                Value::Time(t)
            }
            10 => {
                // Binary
                let bin_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                let b = data[offset..offset + bin_len].to_vec();
                offset += bin_len;
                Value::Binary(b)
            }
            11 => {
                // Json
                let str_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                let s = String::from_utf8_lossy(&data[offset..offset + str_len]).to_string();
                offset += str_len;
                Value::Json(s)
            }
            12 => {
                // Uuid
                let str_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                let s = String::from_utf8_lossy(&data[offset..offset + str_len]).to_string();
                offset += str_len;
                Value::Uuid(s)
            }
            _ => Value::Null,
        };
        values.push(val);
    }

    Ok(Some(values))
}

/// Write a batch of rows to a temp file.
pub fn write_rows(writer: &mut impl Write, rows: &[Vec<Value>]) -> io::Result<()> {
    for row in rows {
        let data = serialize_row(row);
        writer.write_all(&data)?;
    }
    writer.flush()?;
    Ok(())
}

/// Read all rows from a temp file reader.
pub fn read_all_rows(reader: &mut impl Read) -> io::Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    loop {
        match deserialize_row(reader)? {
            Some(row) => rows.push(row),
            None => break,
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_row() {
        let row = vec![
            Value::Integer(42),
            Value::Varchar("hello".into()),
            Value::Null,
            Value::Float(3.14),
            Value::Boolean(true),
            Value::BigInt(i64::MAX),
            Value::DateTime(1234567890),
        ];

        let data = serialize_row(&row);
        let mut cursor = std::io::Cursor::new(data);
        let recovered = deserialize_row(&mut cursor).unwrap().unwrap();
        assert_eq!(recovered.len(), row.len());
        assert_eq!(format!("{:?}", recovered), format!("{:?}", row));
    }

    #[test]
    fn test_write_read_rows() {
        let rows = vec![
            vec![Value::Integer(1), Value::Varchar("a".into())],
            vec![Value::Integer(2), Value::Varchar("b".into())],
            vec![Value::Integer(3), Value::Varchar("c".into())],
        ];

        let mut buf = Vec::new();
        write_rows(&mut buf, &rows).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let recovered = read_all_rows(&mut cursor).unwrap();
        assert_eq!(recovered.len(), 3);
    }

    #[test]
    fn test_temp_file_cleanup() {
        let mut mgr = TempFileManager::new().unwrap();
        let path = {
            let tf = mgr.create_file("test").unwrap();
            let mut w = tf.writer().unwrap();
            w.write_all(b"hello").unwrap();
            w.flush().unwrap();
            tf.path().to_path_buf()
        };
        assert!(path.exists());
        drop(mgr);
        assert!(!path.exists());
    }
}
