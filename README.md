# dariadb - numeric time-series database.

# Continuous Integration

|  version | build & tests | test coverage |
|---------------------|---------|----------|
| `master`   | [![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=master)](https://travis-ci.org/lysevi/dariadb) |  [![codecov](https://codecov.io/gh/lysevi/dariadb/branch/master/graph/badge.svg)](https://codecov.io/gh/lysevi/dariadb) |
| `develop` | [![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=dev)](https://travis-ci.org/lysevi/dariadb) | [![codecov](https://codecov.io/gh/lysevi/dariadb/branch/dev/graph/badge.svg)](https://codecov.io/gh/lysevi/dariadb) |
 

# Features
* True columnar storage
* Can be used as a server application or an embedded library.
* Full featured http api.
* Accept unordered data.
* Each measurement contains:
  - Id - x32 unsigned integer value.
  - Time - x64 timestamp.
  - Value - x64 float.
  - Flag - x32 unsigned integer.
* Write strategies:
  - wal - little cache and all values storing to disk in write ahead log. optimised for big write load(but slower than 'memory' strategy).
  - compressed - all values compressed for good disk usage without writing to sorted layer.
  - memory - all values stored in memory and dropped to disk when memory limit is ended.
  - cache - all values stored in memory with writes to disk.
  - memory-only - all valeus stored only in memory.
* LSM-like storage struct with three layers:
  - Memory cache or Append-only files layer, for fast write speed and crash-safety(if strategy is 'wal').
  - Old values stored in compressed block for better disk space usage.
* High write speed:
  - as embedded engine - to disk - 1.5 - 3.5 millions values per second to disk
  - as memory storage(when strategy is 'memory') - 7-9 millions.
  - across the network - 700k - 800k values per second
* Shard-engine: you can split values per shard in disk, for better compaction and read speed up.
* Crash recovery.
* CRC32 for all values.
* Two variants of API:
  - Functor API (async) -  engine apply given function to each measurement in the incoming request.
  - Standard API - You can Query interval as list or values in time point as dictionary.
* Compaction old data with filtration support;
* Statistic:
  - time min/max
  - value min/max
  - measurement count
  - values sum
* Statistical functions: 
  - average
  - median
  - sigma(standard deviation)
  - percentile90
  - percentile99

# Usage example
- See folder "examples"
- How to use dariadb as a embedded storage engine: [dariadb-example](	)

# Dependencies
* Boost 1.54.0 or higher: system, filesystem, date_time,regex, program_options, asio.
* cmake 3.1 or higher
* c++ 14/17 compiler (MSVC 2015, gcc 6.0, clang 3.8)

## Build
---

### Install dependencies

```shell
$ sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install -y libboost-dev  libboost-filesystem-dev libboost-program-options-dev libasio-dev libboost-date-time-dev cmake  g++-6  gcc-6 cpp-6 
$ export CC="gcc-6"
$ export CXX="g++-6"
```

### Jemalloc
Optionaly you can install jemalloc for better memory usage. 
```shell
$ sudo apt-get install libjemalloc-dev
```

Or you may use builtin jemalloc source in dariadb  - just add build option **-DSYSTEM_JEMALLOC=OFF**

### Git submodules
```shell
$ cd dariadb
$ git submodules init 
$ git submodules update
```
### Available build options
- **DARIADB_ENABLE_TESTS** - Enable testing of the dariadb. - ON
- **DARIADB_ENABLE_METRICS** - Enable code metrics. - ON
- **DARIADB_ENABLE_INTEGRATION_TESTS** - Enable integration test. - ON
- **DARIADB_ENABLE_SERVER** - Enable build dariadb server. - ON
- **DARIADB_ENABLE_BENCHMARKS** - Enable build dariadb benchmarks. - ON
- **DARIADB_ENABLE_SAMPLES** - Build dariadb sample programs. - ON
- **DARIADB_ASAN_UBSAN**  - Enable address & undefined behavior sanitizer for binary. - OFF
- **DARIADB_MSAN** - Enable memory sanitizer for binary. - OFF
- **DARIADB_SYSTEM_JEMALLOC** - Use jemalloc installed in the system. - ON
- **DARIADB_ENABLE_TOOLS** - Build utility tools. - ON

#### Configure to build with all benchmarks, but without tests and server.
---
```shell
$ cmake  -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTS=OFF -DENABLE_INTEGRATION_TESTS=OFF -DENABLE_BENCHMARKS=ON -DENABLE_SERVER=OFF . 
```

### clang
---
Clang currently does not supported.
```shell
$ cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE} -stdlib=libc++" -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} -lstdc++" .
$ make
```

### gcc
---
```shell
$ cmake -DCMAKE_BUILD_TYPE=Release .
$ make
```

### Microsoft Visual Studio
---
```cmd
$ cmake -G "Visual Studio 14 2015 Win64" .
$ cmake --build .
```
if you want to build benchmarks and tests
```cmd
$ cmake -G "Visual Studio 14 2015 Win64" -DBUILD_SHARED_LIBS=FALSE  .
$ cmake --build .
```

### build with non system installed boost
---
```shell
$ cmake  -DCMAKE_BUILD_TYPE=Release -DBOOST_ROOT="path/to/boost/" .
$ make
```

