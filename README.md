[![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=dev)](https://travis-ci.org/lysevi/dariadb)
[![Coverage Status](https://coveralls.io/repos/github/lysevi/dariadb/badge.svg?branch=dev)](https://coveralls.io/github/lysevi/dariadb?branch=dev)

# dariadb

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

