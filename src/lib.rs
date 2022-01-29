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
    marker::PhantomData,
    mem,
    path::Path,
    sync::atomic::AtomicU64,
};

use generic_array::{ArrayLength, GenericArray};
use memmap::{MmapMut, MmapOptions};
use thiserror::Error;

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
const MAGIC_BYTES: [u8; 16] = [
    b'S', b'T', b'U', b'F', b'F', b'E', b'R', b'_', b'S', b'H', b'A', b'C', b'K', b'_', b'_', b'_',
];
const ENDIANNESS_CHECK_CONST: u32 = 0xA1B2C3D4;

#[derive(Debug)]
struct StufferShack<N: ArrayLength<u8>> {
    /// Maps a key to an offset.
    index: HashMap<GenericArray<u8, N>, DbLen>,
    /// Internal data map.
    data: MmapMut,
    _key: PhantomData<N>,
}

impl<N> StufferShack<N>
where
    N: ArrayLength<u8>,
    N::ArrayType: Copy,
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
        backing_file.set_len(MAP_SIZE as u64)?;
        backing_file.flush()?;

        let data = unsafe { MmapOptions::new().len(MAP_SIZE).map_mut(&backing_file)? };

        // TODO: Probably not necessary? Forgetting the backing file, so it won't be closed here.
        mem::forget(backing_file);

        Self::new(data, file_len == 0)
    }

    fn open_ephemeral(size: usize) -> io::Result<Self> {
        let data = unsafe { MmapOptions::new().len(size).map_anon()? };
        Self::new(data, true)
    }

    fn new(mut data: MmapMut, needs_init: bool) -> io::Result<Self> {
        // let header = &mut data[0..DB_HEADER_SIZE];

        // let mut index = HashMap::new();
        // if dbg!(needs_init) {
        //     // Database not initialized, write the magic bytes and initial length.
        //     header[0..MAGIC_BYTES_LEN].copy_from_slice(&MAGIC_BYTES);
        //     let initial_len: DbLen = 0;
        //     header[MAGIC_BYTES_LEN..].copy_from_slice(&initial_len.to_le_bytes());
        // } else if &header[0..MAGIC_BYTES_LEN] != &MAGIC_BYTES[..] {
        //     return Err(io::Error::new(
        //         io::ErrorKind::Other,
        //         "database has invalid magic header",
        //     ));
        // }

        // // We're already initialized, so walk entire data to restore the index.
        // let total_size = store_length(&data) as usize;
        // let mut cur = DB_HEADER_SIZE;
        // while cur < total_size {
        //     let record = load_record::<K>(&data, cur as u64);
        //     // length, hash, data. We only need the hash.
        //     // TODO: Unsafe-cast record header instead.
        //     let hash_bytes = &record[ITEM_LEN_SIZE..(ITEM_LEN_SIZE + mem::size_of::<K>())];

        //     // TODO: Find something better (moot with record header).
        //     let hash_ptr: *const K = hash_bytes.as_ptr() as *const K;
        //     let hash = unsafe { *hash_ptr };

        //     index.insert(hash, cur as DbLen);
        //     cur += record.len();
        // }
        // dbg!(index.len());

        // Ok(StufferShack {
        //     index,
        //     data,
        //     _key: PhantomData,
        // })
        todo!()
    }

    fn size(&self) -> u64 {
        store_length(&self.data)
    }

    /// Store the length of the db without the header in the db header.
    fn write_store_length(&mut self, size: DbLen) {
        todo!()
        // let dest = &mut self.data[MAGIC_BYTES_LEN..(MAGIC_BYTES_LEN + DB_LEN_SIZE)];
        // dest.copy_from_slice(&size.to_le_bytes());
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
    fn write(&mut self, key: GenericArray<u8, N>, value: &[u8]) {
        assert!(
            self.index.get(&key).is_none(),
            "rewriting keys is not supported"
        );

        // Get insertion point.
        let insertion_point = todo!();
        let next_insertion_point =
            write_record::<N>(&mut self.data, insertion_point, key.as_ref(), value);

        // Update index.
        self.index.insert(key, insertion_point);

        // Note: By updating the insertion point here, we gain some sort of transactional durability. Alternatively we can increase the insertion point sooner to gain parallel writes. (TODO) Adding a second insertion pointer would give us both.
        todo!("update insertion point");
    }

    fn read(&self, key: &GenericArray<u8, N>) -> Option<&[u8]> {
        let data_offset = *self.index.get(key)?;
        todo!()
        // let record = load_record::<K>(&self.data, data_offset);

        // let value_slice = &record[(ITEM_LEN_SIZE + mem::size_of::<K>())..];
        // Some(value_slice)
    }
}

/// Retrieves the length of the db without header from the db header.
fn store_length(data: &MmapMut) -> DbLen {
    todo!()
    // DbLen::from_le_bytes(
    //     data[MAGIC_BYTES_LEN..(MAGIC_BYTES_LEN + DB_LEN_SIZE)]
    //         .try_into()
    //         .unwrap(),
    // )
}

/// Converts a database offset into a memory offset, which includes the header.
fn data_offset_to_memory_offset(offset: DbLen) -> usize {
    offset as usize + mem::size_of::<DatabaseHeader>()
}

/// Database header.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct DatabaseHeader {
    // Magic bytes, see `MAGIC_BYTES`.
    magic_bytes: [u8; 16],
    // The value `ENDIANNESS_CHECK_CONST` (will be encoded using native endianness).
    endianness_check: u32,
    // Database version. Currently must be 1.
    version: u32,
    // The insertion pointer for new values.
    insertion_pointer: u32,
    /// The size of a key.
    key_length: u16,
    // Extra header space, intentionally left blank for future versions.
    _padding: [u8; 34],
}

#[derive(Copy, Clone, Debug, Error)]
enum InvalidDatabaseError {
    /// First bytes were not equal to the magic file header.
    #[error("invalid magic at start of file")]
    InvalidMagic,
    /// The endianness constant found in the header differed from the stored one.
    #[error("database failed endianness check")]
    EndiannessMismatch,
    /// Version mismatch.
    #[error("version not supported: {version}")]
    UnsupportedVersion {
        /// The version found in the database file.
        version: u32,
    },
    /// The compile-time configured key length does not match opened db.
    #[error("key length mismatch (expected {expected}, actual {actual}")]
    KeyLengthMismatch {
        /// Version that was expected, based on how the database was instantiated.
        expected: u16,
        /// Version found in the database.
        actual: u16,
    },
    /// The key length given at compile time is too large to fit a `u16`.
    #[error("key length overflow")]
    KeyLengthOverflow,
}

impl DatabaseHeader {
    /// Checks that the header is valid for keys with the specified size.
    fn is_valid<N>(&self) -> Result<(), InvalidDatabaseError>
    where
        N: ArrayLength<u8>,
        N::ArrayType: Copy,
    {
        let key_length = mem::size_of::<GenericArray<u8, N>>();

        // Sanity check to ensure all of our data structures have the right size.
        assert_eq!(mem::size_of::<DatabaseHeader>(), 64);
        assert_eq!(
            mem::size_of::<RecordHeader<N>>(),
            // Four bytes (for the offset pointer) + the actual length of the array.
            4 + key_length
        );

        if self.magic_bytes != MAGIC_BYTES {
            return Err(InvalidDatabaseError::InvalidMagic);
        }

        if self.endianness_check != ENDIANNESS_CHECK_CONST {
            return Err(InvalidDatabaseError::EndiannessMismatch);
        }

        if self.version != 1 {
            return Err(InvalidDatabaseError::UnsupportedVersion {
                version: self.version,
            });
        }

        if key_length > u16::MAX as usize {
            return Err(InvalidDatabaseError::KeyLengthOverflow);
        }

        if self.key_length != key_length as u16 {
            return Err(InvalidDatabaseError::KeyLengthMismatch {
                expected: key_length as u16,
                actual: self.key_length,
            });
        }

        Ok(())
    }
}

/// Record header.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
struct RecordHeader<N>
where
    N: ArrayLength<u8>,
    N::ArrayType: Copy,
{
    /// The length of the data value.
    value_length: u32,
    /// The key, typically a hash.
    key: GenericArray<u8, N>,
}

/// Retrieve a record (with header) at offset.
///
/// Given a specific data offset, returns the record header and data slice.
#[inline]
fn record_at_offset<N>(data: &MmapMut, data_offset: DbLen) -> (&RecordHeader<N>, &[u8])
where
    N: ArrayLength<u8>,
    N::ArrayType: Copy,
{
    let header_size = mem::size_of::<RecordHeader<N>>();

    let start = data_offset_to_memory_offset(data_offset);
    let header_ptr = start as *const RecordHeader<N>;

    // TODO: FIX POTENTIAL ALIGNMENT ISSUES.
    let header = unsafe { header_ptr.as_ref() }.expect("DID YOU FIX THE ALIGNMENT ISSUES?");

    let value_slice = &data[start..(start + header_size)];
    (header, value_slice)
}

/// Write a record at specified location.
///
/// Returns the next available `data_offset` after the write.
fn write_record<N>(data: &mut MmapMut, data_offset: DbLen, key: &[u8], value: &[u8]) -> DbLen
where
    N: ArrayLength<u8>,
    N::ArrayType: Copy,
{
    let header_size = mem::size_of::<RecordHeader<N>>();
    let start = data_offset_to_memory_offset(data_offset);
    let header_ptr = start as *mut RecordHeader<N>;

    // TODO: FIX POTENTIAL ALIGNMENT ISSUES.
    let header = unsafe { header_ptr.as_mut() }.expect("DID YOU FIX THE ALIGNMENT ISSUES?");
    assert!(
        value.len() < u32::MAX as usize,
        "value too large to be stored"
    );
    header.value_length = value.len() as u32;
    header.key.copy_from_slice(key);

    let value_slice = &mut data[start..(start + header_size)];
    value_slice.copy_from_slice(value);

    data_offset + header_size as DbLen + value.len() as DbLen
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
