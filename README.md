# dariadb - numeric time-series database.

# Continuous Integration

|  version | build & tests | test coverage |
|---------------------|---------|----------|
| `master`   | [![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=master)](https://travis-ci.org/lysevi/dariadb) |  [![Coverage Status](https://coveralls.io/repos/github/lysevi/dariadb/badge.svg?branch=master)](https://coveralls.io/github/lysevi/dariadb?branch=master) |
| `develop` | [![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=dev)](https://travis-ci.org/lysevi/dariadb) | [![Coverage Status](https://coveralls.io/repos/github/lysevi/dariadb/badge.svg?branch=dev)](https://coveralls.io/github/lysevi/dariadb?branch=dev)

# Features
* Full featured server with client library(C++).
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
* High write speed(2.5 - 3.5 millions values per second) in using as library.
* High write speed(150k - 200k values per second) across the network.
* Crash recovery.
* CRC32 for all values.
* Two variants of API:
  - Functor API -  engine apply given function to each measurement in the incoming request.
  - Standard API - You can Query interval as list or values in time point as dictionary.
* Write strategies:
  - fast write - optimised for big write load.
  - fast read  - values stored in trees for fast search.
  - compressed - all values compressed for good disk usage without writing to sorted layer.
  - dynamic - values are compressed, if the level is more than a certain value, or for a certain period of time.

# Dependencies
* Boost 1.53.0 or higher: system, filesystem, interprocess (mmap), unit_test_framework(to build tests), program_options, asio, log and regex(for server only)
* cmake 3.1 or higher
* c++ 14/17 compiler (MSVC 2015, gcc 6.0, clang 3.8)

##Build
### Install dependencies
---
```shell
$ sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
$ sudo apt-get update
$ sudo apt-get install -y libboost-dev  libboost-filesystem-dev libboost-test-dev libboost-program-options-dev libasio-dev libboost-log-dev libboost-regex-dev cmake  g++-6  gcc-6 cpp-6
$ export CC="gcc-6"
$ export CXX="g++-6"
```

###clang
---
```shell
$ cmake -DCMAKE_BUILD_TYPE=Releae -DCMAKE_CXX_FLAGS_RELEASE="${CMAKE_CXX_FLAGS_RELEASE} -stdlib=libc++" -DCMAKE_EXE_LINKER_FLAGS="${CMAKE_EXE_LINKER_FLAGS} -lstdc++" .
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

