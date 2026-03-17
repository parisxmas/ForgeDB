use crate::common::{PageId, PAGE_SIZE};

/// A fixed-size page that serves as the basic unit of storage.
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
    pub is_dirty: bool,
    pub pin_count: u32,
}

impl Page {
    /// Create a new page with the given id and zeroed data.
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: [0u8; PAGE_SIZE],
            is_dirty: false,
            pin_count: 0,
        }
    }

    /// Reset this page frame for reuse with a new page id.
    pub fn reset(&mut self, id: PageId) {
        self.id = id;
        self.data = [0u8; PAGE_SIZE];
        self.is_dirty = false;
        self.pin_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = Page::new(PageId(0));
        assert_eq!(page.id, PageId(0));
        assert_eq!(page.data, [0u8; PAGE_SIZE]);
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count, 0);
    }

    #[test]
    fn test_reset_page() {
        let mut page = Page::new(PageId(0));
        page.data[0] = 0xFF;
        page.is_dirty = true;
        page.pin_count = 3;

        page.reset(PageId(5));

        assert_eq!(page.id, PageId(5));
        assert_eq!(page.data[0], 0);
        assert!(!page.is_dirty);
        assert_eq!(page.pin_count, 0);
    }
}
