//! Efficient WAL-only storage.
//!
//! Data format:
//!
//! Record := Length || Hash || Value
//! WAL := [Record]
//!
//! Overhead per stored value on disk is 4 bytes per record.

pub mod error;
mod headers;
mod unchecked_cast;

use std::{
    collections::HashMap,
    fs,
    io::{Seek, SeekFrom, Write},
    marker::PhantomData,
    mem,
    path::Path,
};

use error::{InvalidDatabaseError, StufferShackError};
use generic_array::{ArrayLength, GenericArray};
use memmap::{MmapMut, MmapOptions};

use crate::{
    headers::{DatabaseHeader, RecordHeader},
    unchecked_cast::{UncheckedCast, UncheckedCastMut},
};

// TODO: Use im-rs for parallel read/write.
// TODO: Use serialization of in-memory index, storing offset, to allow fast recovery of WAL.
// TODO: Persist write offset.
// TODO: Consider packing.

const MAP_SIZE: usize = u32::MAX as usize; // TODO: Make this configurable.

/// An append-only database with fixed keys.
#[derive(Debug)]
pub struct StufferShack<N: ArrayLength<u8>> {
    /// Maps a key to an offset.
    index: HashMap<GenericArray<u8, N>, u64>,
    /// Internal data map.
    data: MmapMut,
    /// Phantom data to record key length.
    _key: PhantomData<N>,
}

impl<N> StufferShack<N>
where
    N: ArrayLength<u8>,
    N::ArrayType: Copy,
{
    /// Opens a database that is backed by a file on the filesystem.
    pub fn open_disk<P: AsRef<Path>>(db: P) -> Result<Self, StufferShackError> {
        let mut backing_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db)
            .map_err(StufferShackError::DatabaseOpen)?;

        // Determine the length of the existing file.
        let file_len = backing_file
            .seek(SeekFrom::End(0))
            .map_err(StufferShackError::DatabaseOpen)?;
        backing_file
            .seek(SeekFrom::Start(0))
            .map_err(StufferShackError::DatabaseOpen)?;

        // Truncate the file to the maximum length. This may need to be made configurable, as they allocated file size varies between operation systems, depending on whether they support sparse files. Additionally, while this is necessary on OS X, it is unnecessary on Linux.
        backing_file
            .set_len(MAP_SIZE as u64)
            .map_err(StufferShackError::DatabaseOpen)?;
        backing_file
            .flush()
            .map_err(StufferShackError::DatabaseOpen)?;

        let data = unsafe { MmapOptions::new().len(MAP_SIZE).map_mut(&backing_file) }
            .map_err(StufferShackError::DatabaseOpen)?;

        // TODO: Probably not necessary? We forget the backing file, so it won't be closed on drop.
        mem::forget(backing_file);

        Self::new(data, file_len == 0).map_err(StufferShackError::DatabaseInit)
    }

    /// Opens an in-memory database not backed by a file.
    pub fn open_ephemeral(size: usize) -> Result<Self, StufferShackError> {
        let data = MmapOptions::new()
            .len(size)
            .map_anon()
            .map_err(StufferShackError::DatabaseOpen)?;
        Self::new(data, true).map_err(StufferShackError::DatabaseInit)
    }

    /// Creates a new stuffer shack database.
    ///
    /// If `needs_init` is true, a database header will be written immediately. Otherwise, an
    /// existing header is assumed to exist and will be checked.
    fn new(mut data: MmapMut, needs_init: bool) -> Result<Self, InvalidDatabaseError> {
        if needs_init {
            let new_header: &mut DatabaseHeader = data.at_mut(0);
            new_header.reset::<N>();
        }

        let header: &DatabaseHeader = data.at(0);
        header.check_valid::<N>()?;

        let mut index = HashMap::new();

        // Walk entire data to restore the index.
        let mut cur = mem::size_of::<DatabaseHeader>() as u64;
        while cur < header.next_insert {
            let record_header: &RecordHeader<N> = data.at(cur as usize);
            index.insert(record_header.key, cur);
            cur += record_header.value_length as u64;
        }

        Ok(StufferShack {
            index,
            data,
            _key: PhantomData,
        })
    }

    /// Returns the length of the data store.
    pub fn size(&self) -> u64 {
        self.data.at::<DatabaseHeader>(0).next_insert
    }

    /// Writes a value to the database.
    ///
    /// # Panic
    ///
    /// Panics if `value` is bigger than `u32::MAX`.
    #[inline]
    pub fn write(&mut self, key: GenericArray<u8, N>, value: &[u8]) {
        assert!(value.len() < u32::MAX as usize, "value to large to insert");

        let insertion_point = self.data.at::<DatabaseHeader>(0).next_insert;
        let record_header: &mut RecordHeader<N> = self.data.at_mut(insertion_point as usize);

        record_header.key = key;
        record_header.value_length = value.len() as u32;

        // Copy actual value.
        let value_start = insertion_point as usize + mem::size_of::<RecordHeader<N>>();
        let value_end = value_start + value.len();
        self.data[value_start..value_end].copy_from_slice(value);

        // We're done, update insertion point pointer. This is done _after_ writing the value, so
        // in the event of a crash, we lose this write, but not database integrity.
        //
        // If we ever want to enforce alignment, here's the place to do it.
        let db_header = self.data.at_mut::<DatabaseHeader>(0);
        db_header.next_insert = value_end as u64;

        // We have written the entire value, now update the index.
        self.index.insert(key, insertion_point);
    }

    /// Reads a value from the database.
    #[inline]
    pub fn read(&self, key: &GenericArray<u8, N>) -> Option<&[u8]> {
        let record_offset = *self.index.get(key)?;

        let header: &RecordHeader<N> = self.data.at(record_offset as usize);
        let data_offset = record_offset as usize + mem::size_of::<RecordHeader<N>>();
        let value_slice = &self.data[data_offset..(data_offset + header.value_length as usize)];

        Some(value_slice)
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::StufferShack;
    use generic_array::{typenum::U32, GenericArray};
    use proptest::{proptest, strategy::Strategy};
    use proptest_derive::Arbitrary;
    use rand::{Rng, SeedableRng};

    type Key = GenericArray<u8, U32>;
    type Shack = StufferShack<U32>;

    #[derive(Debug, Arbitrary)]
    struct WriteReadTask {
        key: [u8; 32],
        raw_value: [u8; 32],
        #[proptest(strategy = "0usize..32")]
        len: usize,
    }

    impl WriteReadTask {
        fn key(&self) -> Key {
            self.key.try_into().unwrap()
        }

        fn value(&self) -> &[u8] {
            &self.raw_value[0..self.len]
        }
    }

    proptest! {
        #[test]
        fn write_read_32_times(tasks: [WriteReadTask; 32]) {
            let mut shack: Shack = StufferShack::open_ephemeral(200*1024*1024).unwrap();

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
        let mut shack: Shack = StufferShack::open_ephemeral(1024 * 1024 * 1024).unwrap();
        // let mut shack: Shack = StufferShack::open_disk("test.shack").unwrap();

        let mut total_payload = 0usize;

        // First, write entries.
        let data = DataGen::new();
        for (key, value) in data.take(count) {
            total_payload += key.len() + value.len();

            shack.write(key.into(), value);
        }

        // Read back and verify entries.
        let data = DataGen::new();
        for (key, value) in data.take(count) {
            let read_value = shack.read(&key.into());
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
