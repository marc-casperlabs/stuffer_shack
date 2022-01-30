# stuffer_shack

Stuffer shack is a data store intended for medium amounts of append-only data. It stores all data on disk in a simple write log, and keeps a reconstructible in-memory index to look it up. The ideal input for `stuffer_shack` is content-addressed data that is never deleted.

It was originally written to replace LMDB *in a very specific use case*: If data is never altered or deleted and keys are completely random, `stuffer_shack` should be able to lower disk usage and underlying write operations compared to LMDB. If this is not true for your data, please reconsider using `stuffer_shack`.

## Features

* **Zero-copy** (via mmap): The backing store of `stuffer_shack` is a memory mapped file containing both keys and values. Any read from the shack returns a slice in that region.
* **Time-based data locality**: All values are stored sequentially in the order they are written, avoiding the problem of random keys (like hashes) maximizing distance between related entries.
* **Compactness**: The overhead for storing a value on disk is only 4 bytes.
* **Sequential disk writes**: Data is written to disk in a maximally sequential manner, only database header is ever rewritten. This reduces the number of IOOPS to a minimum, which may be at a premium in cloud-based storage.
* **O(1) read and write performance**: No data must ever be reallocated or reordered after a write. Any read is only dependent on a single `HashMap` lookup.
* **Unlimited lock-free concurrent reads**: No lock needs to be acquired to read data.
* **Durability**: The database cannot be corrupted due to power outages, only unfinished writes will be lost.

## Limitations

Due to its specialized data model, `stuffer_shack` has quite a range of limitations:

* **Memory bound**: The entire index is kept in memory, requiring roughly `key_size + 4` bytes of memory per stored key to operate (i.e. a database with 10 million items and 32 byte keys will use at least 360 megabytes of RAM).
* **No disk reclaiming**: Any value written will stay in the write log forever and may end up unreachable if its key is overwritten.
* **No deletion support**: While a key can be overwritten, deletion is not possible.
* **No stored index**: The index must be reconstructed every time the database is loaded.
* **Limited value size**: Values can be no larger than 4 GB.
* **No read transactions**: Reads cannot be batched to ensure a consistent view of the world when reading multiple values.
* **No write transactions**: Every write is executed and persisted immediately, with no rollback or multi-write atomicity guarantees.
* **No alignment enforcement**: For maximal compactness, data is written unaligned to disk and memory. As a result, access may be slowed down when reading unaligned values.
* **Endianness dependency**: The database always uses host endianness internally. Currently it is not possible to copy a database to a machine with a different CPU architecture that has different endianness.
* **No integrity checks**: All data is untyped and no user-defined conditions can be enforced.

## Roadmap

This project does not have a detailed roadmap yet, but you can have a look at

* [Current issues](https://github.com/marc-casperlabs/stuffer_shack/labels/bug)
* [Proposed new features](https://github.com/marc-casperlabs/stuffer_shack/labels/enhancement)
