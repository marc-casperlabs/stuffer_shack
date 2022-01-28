//! Efficient WAL-only storage.
//!
//! Data format:
//!
//! Record := Length || Hash || Value
//! WAL := [Record]
//!
//! Overhead per stored value on disk is 4 bytes per record.

use std::{
    collections::HashMap,
    fs,
    hash::Hash,
    io::{self, Seek, SeekFrom, Write},
    mem,
    path::Path,
    sync::atomic::AtomicU64,
};

use memmap::{MmapMut, MmapOptions};

// TODO: Use im-rs for parallel read/write.
// TODO: Use serialization of in-memory index, storing offset, to allow fast recovery of WAL.
// TODO: Persist write offset.
// TODO: Consider packing.

// const MAP_SIZE: usize = usize::MAX / 2;
const MAP_SIZE: usize = u32::MAX as usize; // TODO: Figure out why allocation fails.

type ItemLen = u32;
type DbLen = u64;
const ITEM_LEN_SIZE: usize = mem::size_of::<ItemLen>();
const DB_LEN_SIZE: usize = mem::size_of::<DbLen>();
const MAGIC_BYTES: [u8; 64] = [
    b'S', b'T', b'U', b'F', b'F', b'E', b'R', b'_', b'S', b'H', b'A', b'C', b'K', b'_', b'_', b'_',
    b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_',
    b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_',
    b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_', b'_',
];
const MAGIC_BYTES_LEN: usize = 64;
const DB_HEADER_SIZE: usize = DB_LEN_SIZE + MAGIC_BYTES_LEN;

#[derive(Debug)]
struct StufferShack<K> {
    /// Maps a key to an offset.
    index: HashMap<K, DbLen>,
    /// Internal data map.
    data: MmapMut,
}

impl<K> StufferShack<K>
where
    // TODO: Cleanup traits, `K` needs to be a fixed size POD.
    K: Copy + Eq + Hash + AsRef<[u8]>,
{
    fn open_disk<P: AsRef<Path>>(db: P) -> io::Result<Self> {
        let mut backing_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db)?;

        let file_len = backing_file.seek(SeekFrom::End(0))?;
        backing_file.seek(SeekFrom::Start(0))?;

        // TODO: Is this necessary outside OS X?
        backing_file.set_len(dbg!(MAP_SIZE as u64))?;
        backing_file.flush()?;

        let data = unsafe { MmapOptions::new().len(MAP_SIZE).map_mut(&backing_file)? };

        // TODO: Probably not necessary? Forgetting the backing file, so it won't be closed here.
        mem::forget(backing_file);

        // TODO: Write offset.
        Self::new(data, file_len == 0)
    }

    fn open_ephemeral(size: usize) -> io::Result<Self> {
        let data = unsafe { MmapOptions::new().len(size).map_anon()? };
        Self::new(data, true)
    }

    fn new(mut data: MmapMut, needs_init: bool) -> io::Result<Self> {
        let header = &mut data[0..DB_HEADER_SIZE];

        let mut index = HashMap::new();
        eprintln!("checking");
        if dbg!(needs_init) {
            // Database not initialized, write the magic bytes and initial length.
            header[0..MAGIC_BYTES_LEN].copy_from_slice(&MAGIC_BYTES);
            let initial_len: DbLen = 0;
            header[MAGIC_BYTES_LEN..].copy_from_slice(&initial_len.to_le_bytes());
        } else if &header[0..MAGIC_BYTES_LEN] != &MAGIC_BYTES[..] {
            return Err(todo!("magic bytes incorrect"));
        }
        eprintln!("check successful");

        // We're already initialized, so walk entire data to restore the index.
        let total_size = store_length(&data) as usize;
        let mut cur = DB_HEADER_SIZE;
        while cur < total_size {
            let record = load_record::<K>(&data, cur as u64);
            // length, hash, data. We only need the hash.
            // TODO: Unsafe-cast record header instead.
            let hash_bytes = &record[ITEM_LEN_SIZE..(ITEM_LEN_SIZE + mem::size_of::<K>())];

            // TODO: Find something better (moot with record header).
            let hash_ptr: *const K = hash_bytes.as_ptr() as *const K;
            let hash = unsafe { *hash_ptr };

            index.insert(hash, cur as DbLen);
            cur += record.len();
        }
        dbg!(index.len());

        Ok(StufferShack { index, data })
    }

    fn size(&self) -> u64 {
        store_length(&self.data)
    }

    /// Store the length of the db without the header in the db header.
    fn write_store_length(&mut self, size: DbLen) {
        let dest = &mut self.data[MAGIC_BYTES_LEN..(MAGIC_BYTES_LEN + DB_LEN_SIZE)];
        dest.copy_from_slice(&size.to_le_bytes());
    }

    /// Reserves a record in the db with the specified size.
    ///
    /// Returns the data offset and a writable slice.
    fn reserve_record(&mut self, record_size: ItemLen) -> (DbLen, &mut [u8]) {
        let old_store_length = store_length(&self.data);
        let new_store_length = old_store_length + record_size as DbLen;
        self.write_store_length(new_store_length);
        let data = &mut self.data[data_offset_to_memory_offset(old_store_length)
            ..data_offset_to_memory_offset(new_store_length)];
        (old_store_length, data)
    }

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
        assert!(value_len <= ItemLen::MAX as usize);

        // Format: LENGTH(32) + KEY + VALUE
        let record_len = calc_record_len::<K>(value_len as u32);
        let (record_offset, record) = self.reserve_record(record_len.try_into().unwrap());

        // TODO: Optimize range checks.

        // Write length to record.
        record[0..ITEM_LEN_SIZE].copy_from_slice(&(value_len as ItemLen).to_le_bytes());

        let value_offset = ITEM_LEN_SIZE + mem::size_of::<K>();
        record[ITEM_LEN_SIZE..value_offset].copy_from_slice(key.as_ref());
        record[value_offset..].copy_from_slice(value);

        // Update index.
        self.index.insert(key, record_offset);
    }

    fn read(&self, key: &K) -> Option<&[u8]> {
        let data_offset = *self.index.get(key)?;
        let record = load_record::<K>(&self.data, data_offset);

        let value_slice = &record[(ITEM_LEN_SIZE + mem::size_of::<K>())..];
        Some(value_slice)
    }
}

/// Retrieves the length of the db without header from the db header.
fn store_length(data: &MmapMut) -> DbLen {
    DbLen::from_le_bytes(
        data[MAGIC_BYTES_LEN..(MAGIC_BYTES_LEN + DB_LEN_SIZE)]
            .try_into()
            .unwrap(),
    )
}

/// Converts a database offset into a memory offset, which includes the header.
fn data_offset_to_memory_offset(offset: DbLen) -> usize {
    offset as usize + DB_HEADER_SIZE
}

/// Loads a record from a specified offset.
fn load_record<K>(data: &MmapMut, data_offset: DbLen) -> &[u8] {
    // Read the length.
    let mem_offset = data_offset_to_memory_offset(data_offset);
    let value_len = ItemLen::from_le_bytes(
        data[mem_offset..(mem_offset + ITEM_LEN_SIZE)]
            .try_into()
            .unwrap(),
    );
    let record_len = calc_record_len::<K>(value_len);

    &data[mem_offset..(mem_offset + record_len as usize)]
}

/// Calculates the length of a record from its data length (by adding key + local header size).
fn calc_record_len<K>(value_len: ItemLen) -> DbLen {
    (ITEM_LEN_SIZE + mem::size_of::<K>() + value_len as usize)
        .try_into()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::StufferShack;
    use proptest::proptest;
    use proptest_derive::Arbitrary;
    use rand::{Rng, SeedableRng};

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
            let mut shack: StufferShack<Key> = StufferShack::open_ephemeral(200*1024*1024).unwrap();

            for task in &tasks {
                shack.write(task.key(), task.value());
            }

            for task in &tasks {
                assert_eq!(shack.read(&task.key()), Some(task.value()))
            }
        }
    }

    #[derive(Clone, Debug)]
    struct DataGen {
        current: usize,
        sizes: Box<[usize]>,
        offsets: Box<[usize]>,
        data: &'static [u8],
        rng: rand_chacha::ChaCha12Rng,
    }

    impl DataGen {
        fn new() -> Self {
            let seed = [0xFF; 32];

            let max_len = 8000usize;
            let limit: usize = 524287; // 7th Mersenne prime.

            let data = (0..max_len).map(|num| num as u8).collect();
            let sizes = Box::new([0usize, 1, 8, 32, 1, 4, 4, 4, 1, 7000, 8, 4]);
            let offsets = Box::new([0, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31]);

            let rng = rand_chacha::ChaCha12Rng::from_seed(seed);

            DataGen {
                current: 0,
                sizes,
                offsets,
                data: Box::leak(data),
                rng,
            }
        }
    }

    impl Iterator for DataGen {
        type Item = ([u8; 32], &'static [u8]);

        fn next(&mut self) -> Option<Self::Item> {
            let size = self.sizes[self.current % self.sizes.len()];
            let offset = self.offsets[self.current % self.offsets.len()];

            let slice = &self.data[offset..(size + offset)];

            self.current += 1;

            Some((self.rng.gen(), slice))
        }
    }

    #[test]
    fn ten_million_entries() {
        let count = 1_000_000;

        // TODO: Do on-disk.
        // let mut shack: StufferShack<Key> =
        // StufferShack::open_ephemeral(1024 * 1024 * 1024).unwrap();
        let mut shack: StufferShack<Key> = StufferShack::open_disk("test.shack").unwrap();

        let mut total_payload = 0usize;

        // First, write entries.
        let data = DataGen::new();
        for (key, value) in data.take(count) {
            total_payload += key.len() + value.len();

            shack.write(key, value);
        }

        // Read back and verify entries.
        let data = DataGen::new();
        for (key, value) in data.take(count) {
            let read_value = shack.read(&key);
            assert_eq!(read_value, Some(value));
        }

        let db_size = shack.size() as usize;
        let overhead = db_size - total_payload;

        println!(
            "total payload {}  db size {}  overhead {}  ratio {}",
            total_payload,
            db_size,
            overhead,
            db_size as f64 / total_payload as f64
        )
    }
}
