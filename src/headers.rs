use std::mem;

use generic_array::{ArrayLength, GenericArray};

use crate::error::InvalidDatabaseError;

/// The magic bytes, indicating that a given file is a stuffer shack db.
const MAGIC_BYTES: [u8; 16] = [
    b'S', b'T', b'U', b'F', b'F', b'E', b'R', b'_', b'S', b'H', b'A', b'C', b'K', b'_', b'_', b'_',
];

/// Magic number used to check endianness.
const ENDIANNESS_CHECK_CONST: u32 = 0xA1B2C3D4;

/// Database header.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct DatabaseHeader {
    // Magic bytes, see `MAGIC_BYTES`.
    pub(crate) magic_bytes: [u8; 16],
    // The value `ENDIANNESS_CHECK_CONST` (will be encoded using native endianness).
    pub(crate) endianness_check: u32,
    // Database version. Currently must be 1.
    pub(crate) version: u32,
    // The insertion pointer for new values.
    pub(crate) insertion_pointer: u32,
    /// The size of a key.
    pub(crate) key_length: u16,
    // Extra header space, intentionally left blank for future versions.
    pub(crate) _padding: [u8; 34],
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
pub(crate) struct RecordHeader<N>
where
    N: ArrayLength<u8>,
    N::ArrayType: Copy,
{
    /// The length of the data value.
    pub(crate) value_length: u32,
    /// The key, typically a hash.
    pub(crate) key: GenericArray<u8, N>,
}
