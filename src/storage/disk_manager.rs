use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::common::{PageId, PAGE_SIZE};
use crate::error::Result;

/// Manages reading and writing pages to a database file on disk.
pub struct DiskManager {
    file: File,
    next_page_id: u32,
}

impl DiskManager {
    /// Open or create a database file at the given path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let next_page_id = (file_size / PAGE_SIZE as u64) as u32;

        Ok(Self {
            file,
            next_page_id,
        })
    }

    /// Read page data from disk into the provided buffer.
    pub fn read_page(&mut self, page_id: PageId, buf: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id.0 as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        // If the file is shorter than the requested offset, fill with zeros.
        let bytes_read = self.file.read(buf)?;
        if bytes_read < PAGE_SIZE {
            buf[bytes_read..].fill(0);
        }
        Ok(())
    }

    /// Write page data from the buffer to disk.
    pub fn write_page(&mut self, page_id: PageId, buf: &[u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id.0 as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        self.file.flush()?;
        Ok(())
    }

    /// Allocate a new page id and extend the file to accommodate it.
    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = PageId(self.next_page_id);
        self.next_page_id += 1;

        // Extend the file by writing a zeroed page.
        let buf = [0u8; PAGE_SIZE];
        self.write_page(page_id, &buf)?;

        Ok(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_allocate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut dm = DiskManager::new(&path).unwrap();

        let p0 = dm.allocate_page().unwrap();
        assert_eq!(p0, PageId(0));

        let p1 = dm.allocate_page().unwrap();
        assert_eq!(p1, PageId(1));
    }

    #[test]
    fn test_read_write_page() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut dm = DiskManager::new(&path).unwrap();

        let page_id = dm.allocate_page().unwrap();

        let mut write_buf = [0u8; PAGE_SIZE];
        write_buf[0] = 0xDE;
        write_buf[1] = 0xAD;
        write_buf[PAGE_SIZE - 1] = 0xFF;
        dm.write_page(page_id, &write_buf).unwrap();

        let mut read_buf = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_buf).unwrap();

        assert_eq!(read_buf[0], 0xDE);
        assert_eq!(read_buf[1], 0xAD);
        assert_eq!(read_buf[PAGE_SIZE - 1], 0xFF);
    }

    #[test]
    fn test_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write with one disk manager instance.
        {
            let mut dm = DiskManager::new(&path).unwrap();
            let page_id = dm.allocate_page().unwrap();
            let mut buf = [0u8; PAGE_SIZE];
            buf[42] = 0xBE;
            dm.write_page(page_id, &buf).unwrap();
        }

        // Read with a fresh disk manager instance.
        {
            let mut dm = DiskManager::new(&path).unwrap();
            // The next page id should reflect the existing file.
            assert_eq!(dm.next_page_id, 1);

            let mut buf = [0u8; PAGE_SIZE];
            dm.read_page(PageId(0), &mut buf).unwrap();
            assert_eq!(buf[42], 0xBE);
        }
    }
}
