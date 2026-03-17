use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};

use crate::common::*;
use crate::error::{ForgeError, Result};

/// WAL record type tags.
const TAG_BEGIN: u8 = 0;
const TAG_COMMIT: u8 = 1;
const TAG_ABORT: u8 = 2;
const TAG_PAGE_WRITE: u8 = 3;
const TAG_CHECKPOINT: u8 = 4;

/// A single write-ahead log record.
#[derive(Debug)]
pub enum WalRecord {
    Begin(TxnId),
    Commit(TxnId),
    Abort(TxnId),
    PageWrite {
        txn_id: TxnId,
        page_id: PageId,
        before_image: Box<[u8; PAGE_SIZE]>,
        after_image: Box<[u8; PAGE_SIZE]>,
    },
    Checkpoint,
}

/// Write-Ahead Log that persists records to a file on disk.
///
/// Record format: `[record_length: u32 LE][type: u8][payload][record_length: u32 LE]`
///
/// The `record_length` stores the length of `(type + payload)`, NOT including the
/// two length fields themselves. The trailing length allows reverse scanning.
pub struct Wal {
    file: File,
    offset: u64,
}

impl Wal {
    /// Open or create a WAL file at the given path, positioned at the end.
    pub fn new(path: &str) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let offset = file.seek(SeekFrom::End(0))?;

        Ok(Self { file, offset })
    }

    /// Serialize and append a WAL record, then flush to disk.
    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let payload = Self::serialize_payload(record);
        let record_length = payload.len() as u32; // type byte + payload content

        // Write leading length.
        self.file.write_all(&record_length.to_le_bytes())?;
        // Write type tag + payload.
        self.file.write_all(&payload)?;
        // Write trailing length.
        self.file.write_all(&record_length.to_le_bytes())?;

        self.file.flush()?;

        let total = 4 + payload.len() as u64 + 4;
        self.offset += total;

        Ok(())
    }

    /// Read all WAL records from the beginning of the file, in forward order.
    pub fn read_all_records(&mut self) -> Result<Vec<WalRecord>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut records = Vec::new();
        let file_len = self.file.metadata()?.len();
        let mut pos: u64 = 0;

        while pos < file_len {
            // Read leading record_length.
            let mut len_buf = [0u8; 4];
            self.file.read_exact(&mut len_buf).map_err(|e| {
                ForgeError::Wal(format!("failed to read record length at offset {}: {}", pos, e))
            })?;
            let record_length = u32::from_le_bytes(len_buf) as usize;

            // Read (type + payload).
            let mut data = vec![0u8; record_length];
            self.file.read_exact(&mut data).map_err(|e| {
                ForgeError::Wal(format!("failed to read record data at offset {}: {}", pos, e))
            })?;

            // Read trailing record_length (for verification / reverse scanning).
            let mut trail_buf = [0u8; 4];
            self.file.read_exact(&mut trail_buf).map_err(|e| {
                ForgeError::Wal(format!(
                    "failed to read trailing length at offset {}: {}",
                    pos, e
                ))
            })?;
            let trail_length = u32::from_le_bytes(trail_buf) as usize;
            if trail_length != record_length {
                return Err(ForgeError::Wal(format!(
                    "WAL record length mismatch at offset {}: leading={}, trailing={}",
                    pos, record_length, trail_length
                )));
            }

            let record = Self::deserialize_payload(&data)?;
            records.push(record);

            pos += 4 + record_length as u64 + 4;
        }

        Ok(records)
    }

    /// Truncate the WAL file (e.g. after a successful checkpoint).
    pub fn truncate(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.offset = 0;
        Ok(())
    }

    // ---- private helpers ----

    /// Serialize a record into its on-disk representation (type tag + payload bytes).
    fn serialize_payload(record: &WalRecord) -> Vec<u8> {
        match record {
            WalRecord::Begin(txn_id) => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(TAG_BEGIN);
                buf.extend_from_slice(&txn_id.0.to_le_bytes());
                buf
            }
            WalRecord::Commit(txn_id) => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(TAG_COMMIT);
                buf.extend_from_slice(&txn_id.0.to_le_bytes());
                buf
            }
            WalRecord::Abort(txn_id) => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(TAG_ABORT);
                buf.extend_from_slice(&txn_id.0.to_le_bytes());
                buf
            }
            WalRecord::PageWrite {
                txn_id,
                page_id,
                before_image,
                after_image,
            } => {
                let mut buf = Vec::with_capacity(1 + 8 + 4 + PAGE_SIZE + PAGE_SIZE);
                buf.push(TAG_PAGE_WRITE);
                buf.extend_from_slice(&txn_id.0.to_le_bytes());
                buf.extend_from_slice(&page_id.0.to_le_bytes());
                buf.extend_from_slice(before_image.as_ref());
                buf.extend_from_slice(after_image.as_ref());
                buf
            }
            WalRecord::Checkpoint => {
                vec![TAG_CHECKPOINT]
            }
        }
    }

    /// Deserialize a record from the on-disk (type tag + payload) bytes.
    fn deserialize_payload(data: &[u8]) -> Result<WalRecord> {
        if data.is_empty() {
            return Err(ForgeError::Wal("empty WAL record".to_string()));
        }

        let tag = data[0];
        let payload = &data[1..];

        match tag {
            TAG_BEGIN => {
                if payload.len() != 8 {
                    return Err(ForgeError::Wal(format!(
                        "Begin record has wrong payload length: {}",
                        payload.len()
                    )));
                }
                let txn_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(WalRecord::Begin(TxnId(txn_id)))
            }
            TAG_COMMIT => {
                if payload.len() != 8 {
                    return Err(ForgeError::Wal(format!(
                        "Commit record has wrong payload length: {}",
                        payload.len()
                    )));
                }
                let txn_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(WalRecord::Commit(TxnId(txn_id)))
            }
            TAG_ABORT => {
                if payload.len() != 8 {
                    return Err(ForgeError::Wal(format!(
                        "Abort record has wrong payload length: {}",
                        payload.len()
                    )));
                }
                let txn_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                Ok(WalRecord::Abort(TxnId(txn_id)))
            }
            TAG_PAGE_WRITE => {
                let expected = 8 + 4 + PAGE_SIZE + PAGE_SIZE;
                if payload.len() != expected {
                    return Err(ForgeError::Wal(format!(
                        "PageWrite record has wrong payload length: {} (expected {})",
                        payload.len(),
                        expected
                    )));
                }
                let txn_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                let page_id = u32::from_le_bytes(payload[8..12].try_into().unwrap());

                let mut before_image = Box::new([0u8; PAGE_SIZE]);
                before_image.copy_from_slice(&payload[12..12 + PAGE_SIZE]);

                let mut after_image = Box::new([0u8; PAGE_SIZE]);
                after_image.copy_from_slice(&payload[12 + PAGE_SIZE..12 + 2 * PAGE_SIZE]);

                Ok(WalRecord::PageWrite {
                    txn_id: TxnId(txn_id),
                    page_id: PageId(page_id),
                    before_image,
                    after_image,
                })
            }
            TAG_CHECKPOINT => {
                if !payload.is_empty() {
                    return Err(ForgeError::Wal(format!(
                        "Checkpoint record has unexpected payload of length {}",
                        payload.len()
                    )));
                }
                Ok(WalRecord::Checkpoint)
            }
            _ => Err(ForgeError::Wal(format!("unknown WAL record tag: {}", tag))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_commit_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut wal = Wal::new(wal_str).unwrap();

        wal.append(&WalRecord::Begin(TxnId(1))).unwrap();
        wal.append(&WalRecord::Commit(TxnId(1))).unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 2);

        match &records[0] {
            WalRecord::Begin(tid) => assert_eq!(tid.0, 1),
            _ => panic!("expected Begin record"),
        }
        match &records[1] {
            WalRecord::Commit(tid) => assert_eq!(tid.0, 1),
            _ => panic!("expected Commit record"),
        }
    }

    #[test]
    fn test_page_write_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut wal = Wal::new(wal_str).unwrap();

        let mut before = Box::new([0u8; PAGE_SIZE]);
        before[0] = 0xAA;
        before[PAGE_SIZE - 1] = 0xBB;

        let mut after = Box::new([0u8; PAGE_SIZE]);
        after[0] = 0xCC;
        after[PAGE_SIZE - 1] = 0xDD;

        wal.append(&WalRecord::PageWrite {
            txn_id: TxnId(42),
            page_id: PageId(7),
            before_image: before,
            after_image: after,
        })
        .unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 1);

        match &records[0] {
            WalRecord::PageWrite {
                txn_id,
                page_id,
                before_image,
                after_image,
            } => {
                assert_eq!(txn_id.0, 42);
                assert_eq!(page_id.0, 7);
                assert_eq!(before_image[0], 0xAA);
                assert_eq!(before_image[PAGE_SIZE - 1], 0xBB);
                assert_eq!(after_image[0], 0xCC);
                assert_eq!(after_image[PAGE_SIZE - 1], 0xDD);
            }
            _ => panic!("expected PageWrite record"),
        }
    }

    #[test]
    fn test_checkpoint_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut wal = Wal::new(wal_str).unwrap();

        wal.append(&WalRecord::Begin(TxnId(1))).unwrap();
        wal.append(&WalRecord::Commit(TxnId(1))).unwrap();
        wal.append(&WalRecord::Checkpoint).unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 3);

        // Truncate the WAL.
        wal.truncate().unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_abort_record() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut wal = Wal::new(wal_str).unwrap();

        wal.append(&WalRecord::Begin(TxnId(5))).unwrap();
        wal.append(&WalRecord::Abort(TxnId(5))).unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 2);

        match &records[1] {
            WalRecord::Abort(tid) => assert_eq!(tid.0, 5),
            _ => panic!("expected Abort record"),
        }
    }

    #[test]
    fn test_multiple_record_types() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut wal = Wal::new(wal_str).unwrap();

        wal.append(&WalRecord::Begin(TxnId(1))).unwrap();
        wal.append(&WalRecord::Begin(TxnId(2))).unwrap();

        let after = Box::new([0xFFu8; PAGE_SIZE]);
        let before = Box::new([0u8; PAGE_SIZE]);
        wal.append(&WalRecord::PageWrite {
            txn_id: TxnId(1),
            page_id: PageId(0),
            before_image: before,
            after_image: after,
        })
        .unwrap();

        wal.append(&WalRecord::Commit(TxnId(1))).unwrap();
        wal.append(&WalRecord::Abort(TxnId(2))).unwrap();
        wal.append(&WalRecord::Checkpoint).unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 6);

        assert!(matches!(&records[0], WalRecord::Begin(TxnId(1))));
        assert!(matches!(&records[1], WalRecord::Begin(TxnId(2))));
        assert!(matches!(
            &records[2],
            WalRecord::PageWrite { txn_id: TxnId(1), page_id: PageId(0), .. }
        ));
        assert!(matches!(&records[3], WalRecord::Commit(TxnId(1))));
        assert!(matches!(&records[4], WalRecord::Abort(TxnId(2))));
        assert!(matches!(&records[5], WalRecord::Checkpoint));
    }
}
