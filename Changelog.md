v0.4.0
=====
- Data compaction methods.
- Less allocations in read queries.
- Auto erase old values.
- Magic numbers in page file.
- Settings:
  - max_chunks_per_page - maximum count chunks per page when write from memory storage.
  - max_store_period - period, when old pages must be removed.
- Wal now contained in-memory index.
- Rename build options: add prefix 'DARIADB_'

v0.3.0
=====
- Remove bystep storage engine.
- Accept unordered data.
- Statistic:
 - time min/max
 - value min/max
 - measurement count
 - values sum
- Page can be repacked.
- Scheme API.
- Implement shard-storage.

v0.2.0
=====
Server:
   - Full featured server with client library(C++).
Storage:
   - Engine version in manifest.
   - Engine write strategies.
   - New algorithm to drop values between levels depending on the strategies.
   - New logger interface.
   - Now can remove old compressed pages.
   - New compression algorithm: speed increased by 3 times.
   - Flag 'source' was removed from measurement.
   - Measurement Id is uint32_t.
   - Remove COLA layer.
   - Speed up dropping data between layers.
   - Old pages compaction.
   - Full futured memory storage.
   - cache-mode - all values stored in memory with async writes to disk.
   - Lockfile in storage.
   - New storage control tool.
   - SQLite3 based manifest.
   - Foreach - is async.
   - Bystep storage.
   - Remove mmap usage.
   - Different folders for 'raw' and 'bystep' values.

v0.1.1
=====
- Code refactoring.
- Compressed level is use "append only" idiom. More crash safety and more faster.
- Use Jenkins hash function for bloom filter.
- Apache License.
- Options file in storage.

v0.1.0
=====
- Async storage.
- Enable filtration by measurement source in interval queries.
- Better restore after crash.
- Two variants of API:
  - Functor API -  engine apply given function to each measurement in the incoming request.
  - Standard API - You can Query interval as list or values in time point as dictionary.
- Speed up read/write .
- API to merge non filled chunks.
- Drop to compressed stored by stored period.

v0.0.7
=====
- Accept unordered data.
- Restore after crash.
- CRC32 checksum for chunks.
- LSM-like struct: COLA layer, Append-only files layer, Compressed layer.
- Bloom filter based on std::hash.

v0.0.6
=====
- base-COLA for sort new measurements.
- new compressed storage.
- LEB128 compression for Id and Flag.
- LRU memory cache.

v0.0.5
======
- Asynchronous writes to disk.
- Asynchronous memory storage.
- PageManager: drop loaded chunks (on get_open_chunks) and dec chunks_in_cur_page() counter.
- PageManager: page fragmentation support.
- Chunk memory pool.

v0.0.4
======
- spinlocks in memory cache.
- B+tree based caches.
- Flush data on write to page.

v0.0.3
======
- speed up.
