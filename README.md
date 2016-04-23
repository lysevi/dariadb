[![Build Status](https://travis-ci.org/lysevi/dariadb.svg?branch=master)](https://travis-ci.org/lysevi/dariadb)

# dariadb


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

