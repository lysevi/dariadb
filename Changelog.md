v0.0.7
=====
- Restore after crash.
- CRC32 checksum for chunks.
- COLA layer.

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
