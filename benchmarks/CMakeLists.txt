INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})

MESSAGE(STATUS "Benchmarks: ")

macro(ADD_BENCHARK name src)
        MESSAGE(STATUS " +" ${name})
        add_executable(${name} ${src} bench_common.h bench_common.cpp)
        
        target_link_libraries(${name} ${DARIADB_STORAGE_LIBS})
        set_target_properties(${name} PROPERTIES FOLDER Benchmarks)
endmacro(ADD_BENCHARK)

macro(ADD_PERF_TEST name)
   MESSAGE(STATUS " + engine_benchmark "  ${name} " "  ${ARGN})
   add_test(${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/engine_benchmark --read-benchmark-runs=10  ${ARGN})
endmacro()

macro(ADD_NT_PERF_TEST name)
   MESSAGE(STATUS " + network_benchmark "  ${name} " "  ${ARGN})
   add_test(${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/network_benchmark --io-threads=5  ${ARGN})
endmacro()

ADD_BENCHARK(engine_benchmark engine_benchmark.cpp)

if(DARIADB_ENABLE_SERVER)
  ADD_BENCHARK(network_benchmark network_benchmark.cpp)
  TARGET_LINK_LIBRARIES(network_benchmark ${DARIADB_CLIENT_LIBS} dariadb-server)
endif(DARIADB_ENABLE_SERVER)


if(DARIADB_ENABLE_INTEGRATION_TESTS)
   MESSAGE(STATUS "Integration tests: ")
   ADD_PERF_TEST(WAL --strategy=WAL)
   ADD_PERF_TEST(COMPRESSED --strategy=COMPRESSED)
   ADD_PERF_TEST(MEMORY --strategy=MEMORY)
   ADD_PERF_TEST(CACHE --strategy=CACHE)
   ADD_PERF_TEST(SHARD_WAL --strategy=WAL --use-shard)
   ADD_PERF_TEST(SHARD_MEM --strategy=MEMORY --use-shard)
   ADD_PERF_TEST(MEMONLY --memory-only  --ids=100 --hours=1  --freq=3)
  if(DARIADB_ENABLE_SERVER)
      ADD_NT_PERF_TEST(ONE_CLIENT  --clients-count=1)
      ADD_NT_PERF_TEST(TEN_CLIENTS  --clients-count=10)
      
      ADD_NT_PERF_TEST(HTTP_ONE_CLIENTS  --clients-count=1  --http-benchmark)
      ADD_NT_PERF_TEST(HTTP_TEN_CLIENTS  --clients-count=10 --http-benchmark)
  endif(DARIADB_ENABLE_SERVER)
endif(DARIADB_ENABLE_INTEGRATION_TESTS)
