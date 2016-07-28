# dariadb - numeric time-series storage engine.

# Continuous Integration

|  version | build & tests | test coverage |
|---------------------|---------|----------|
| `master`   | [![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=master)](https://travis-ci.org/lysevi/dariadb) |  [![Coverage Status](https://coveralls.io/repos/github/lysevi/dariadb/badge.svg?branch=master)](https://coveralls.io/github/lysevi/dariadb?branch=master) |
| `develop` | [![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=dev)](https://travis-ci.org/lysevi/dariadb) | [![Coverage Status](https://coveralls.io/repos/github/lysevi/dariadb/badge.svg?branch=dev)](https://coveralls.io/github/lysevi/dariadb?branch=dev)

# Features
* Each measurement contains:
  - Id - x64 unsigned integer value.
  - Time - x64 timestamp.
  - Value - x64 float.
  - Flag - x32 unsigned integer.
  - Source - x32 unsigned integer.
* Accept unordered data.
* LSM-like storage struct with three layers:
  - Append-only files layer, for fast write speed and crash-safety.
  - For better read speed, data stored in cache oblivious lookahead arrays.
  - Old values stored in compressed block for better disk space usage.
* High write speed(1.5 - 2.5 millions values per second).
* Crash recovery.
* CRC32 for all values.
* Two variants of API:
  - Functor API -  engine apply given function to each measurement in the incoming request.
  - Standard API - You can Query interval as list or values in time point as dictionary.

# Dependencies
* Boost 1.53.0 or higher: system, filesystem, interprocess (mmap), thread, unit_test_framework(to build tests), program_options
* cmake 2.8 or higher
* c++ 11/14 compiler (MSVC 2015, gcc 4.9, clang 3.6)

##build
###clang
---
```shell
$ cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE} -stdlib=libc++" .
$ make
```

###gcc
---
```shell
$ cmake -DCMAKE_BUILD_TYPE=Release .
$ make
```
###on windows with **Microsoft Visual Studio**
---
```cmd
$ cmake -G "Visual Studio 12 2013 Win64" .
$ cmake --build .
```
### build with non system installed boost
---
```shell
$ cmake  -DBOOST_ROOT="path/to/boost/" .
$ make
```

