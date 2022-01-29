//! Various errors used by the crate.

use std::io;

use thiserror::Error;

/// Top-level error for the stuffer shack database crate.
#[derive(Debug, Error)]
pub enum StufferShackError {
    /// Error opening the database.
    #[error("could not open database")]
    DatabaseOpen(#[source] io::Error),
    /// Error initialising the database.
    #[error("database invalid")]
    DatabaseInit(#[source] InvalidDatabaseError),
}

/// A database (header) validation error.
#[derive(Copy, Clone, Debug, Error)]
pub enum InvalidDatabaseError {
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
