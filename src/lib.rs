use std::{collections::HashMap, fs, hash::Hash, io, mem, path::Path, sync::atomic::AtomicUsize};

use memmap::{MmapMut, MmapOptions};

// TODO: Use im-rs for parallel read/write.
// TODO: Use serialization of in-memory index, storing offset, to allow fast recovery of WAL.
// TODO: Persist write offset.

const MAP_SIZE: usize = usize::MAX / 2;

#[derive(Debug)]
struct StufferShack<K> {
    /// Maps a key to an offset.
    index: HashMap<K, (usize, usize)>,
    write_offset: AtomicUsize,
    data: MmapMut,
}

impl<K> StufferShack<K> {
    fn open_disk<P: AsRef<Path>>(db: P) -> io::Result<Self> {
        let backing_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db)?;
        let data = unsafe { MmapOptions::new().len(MAP_SIZE).map_mut(&backing_file)? };
        // TODO: Probably not necessary? Forgetting the backing file, so it won't be closed here.
        mem::forget(backing_file);

        Self::new(data)
    }

    fn open_ephemeral(size: usize) -> io::Result<Self> {
        let data = unsafe { MmapOptions::new().len(size).map_anon()? };
        Self::new(data)
    }

    fn new(data: MmapMut) -> io::Result<Self> {
        // TODO: Restore index.
        Ok(StufferShack {
            index: Default::default(),
            write_offset: AtomicUsize::new(0), // TODO: Load offset from somewhere.
            data,
        })
    }
}

impl<K> StufferShack<K>
where
    K: Eq + Hash,
{
    // TODO: Allow parallel writes.
    fn write(&mut self, key: K, value: &[u8]) {
        // Determine where to write the data.
        // TODO: See if ordering can be relaxed.
        let value_len = value.len();
        let start = self
            .write_offset
            .fetch_add(value_len, std::sync::atomic::Ordering::SeqCst);
        let end = start + value_len;

        // Write data.
        let dest = &mut self.data[start..end];
        dest.copy_from_slice(value);

        // Update index.
        assert!(self.index.get(&key).is_none());
        self.index.insert(key, (start, end));
    }

    fn read(&self, key: &K) -> Option<&[u8]> {
        let (start, end) = self.index.get(key)?;
        let data = &self.data[*start..*end];
        Some(data)
    }
}

#[cfg(test)]
mod tests {
    use super::StufferShack;
    use proptest::proptest;
    use proptest_derive::Arbitrary;

    type Key = [u8; 32];

    #[derive(Debug, Arbitrary)]
    struct WriteReadTask {
        key: Key,
        raw_value: [u8; 32],
        #[proptest(strategy = "0usize..32")]
        len: usize,
    }

    impl WriteReadTask {
        fn key(&self) -> Key {
            self.key
        }

        fn value(&self) -> &[u8] {
            &self.raw_value[0..self.len]
        }
    }

    proptest! {
        #[test]
        fn write_read_32_times(tasks: [WriteReadTask; 32]) {
            let mut shack: StufferShack<Key> = StufferShack::open_ephemeral(100*1024*1024).unwrap();

            for task in &tasks {
                shack.write(task.key(), task.value());
            }

            for task in &tasks {
                assert_eq!(shack.read(&task.key()), Some(task.value()))
            }
        }
    }
}
