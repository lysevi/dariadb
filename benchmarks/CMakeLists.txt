include_directories(../libtimedb)

add_executable (compression_bench compression_bench.cpp)
TARGET_LINK_LIBRARIES(compression_bench libtimedb ${Boost_LIBRARIES})

add_executable (storage_bench storage_bench.cpp)
TARGET_LINK_LIBRARIES(storage_bench libtimedb ${Boost_LIBRARIES})
