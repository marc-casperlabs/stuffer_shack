use std::{collections::HashMap, fs, hash::Hash, io, mem, path::Path, sync::atomic::AtomicUsize};

use memmap::{MmapMut, MmapOptions};

// TODO: Use im-rs for parallel read/write.
// TODO: Use serialization of in-memory index, storing offset, to allow fast recovery of WAL.
// TODO: Persist write offset.

const MAP_SIZE: usize = usize::MAX / 2;
type Length = u32;
const LENGTH_SIZE: usize = mem::size_of::<Length>();

#[derive(Debug)]
struct StufferShack<K> {
    /// Maps a key to an offset.
    index: HashMap<K, usize>,
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
    // TODO: Cleanup traits, `K` needs to be a fixed size POD.
    K: Copy + Eq + Hash + AsRef<[u8]>,
{
    // TODO: Allow parallel writes.
    fn write(&mut self, key: K, value: &[u8]) {
        assert!(
            self.index.get(&key).is_none(),
            "rewriting keys is not supported"
        );

        // Determine where to write the data.
        // TODO: See if ordering can be relaxed.
        // TODO: Ensure conversion is not lossy.
        let value_len = value.len();
        assert!(value_len <= Length::MAX as usize);

        // Format: LENGTH(32) + KEY + VALUE
        let total_len = LENGTH_SIZE + mem::size_of::<K>() + value_len;

        let start_len = self
            .write_offset
            .fetch_add(total_len, std::sync::atomic::Ordering::SeqCst);

        let start_key = start_len + LENGTH_SIZE;
        let start_value = start_key + mem::size_of::<K>();

        // TODO: Optimize range checks.
        // Write length, key, data.
        let slice_len = &mut self.data[start_len..start_key];
        slice_len.copy_from_slice(&(value_len as u32).to_le_bytes());

        let slice_key = &mut self.data[start_key..start_value];
        slice_key.copy_from_slice(key.as_ref());

        let slice_value = &mut self.data[start_value..(start_value + value_len)];
        slice_value.copy_from_slice(value);

        // Update index.
        self.index.insert(key, start_len);
    }

    fn read(&self, key: &K) -> Option<&[u8]> {
        let start = *self.index.get(key)?;

        let value_length_slice = &self.data[start..(start + LENGTH_SIZE)];
        let value_length = Length::from_le_bytes(value_length_slice.try_into().unwrap()) as usize;

        // We skip the key (although we could check it here if wanted to...)
        let value_start = start + LENGTH_SIZE + mem::size_of::<K>();
        let value_end = value_start + value_length;

        let data = &self.data[value_start..value_end];
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
