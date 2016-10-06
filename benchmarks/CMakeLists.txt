INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})

MESSAGE(STATUS "Benchmarks: ")

macro(ADD_BENCHARK name src)
        MESSAGE(STATUS " +" ${name})

        add_executable(${name} ${src} bench_common.h)

        target_link_libraries(${name} ${DARIADB_STORAGE_LIBS})

        set_target_properties(${name} PROPERTIES FOLDER Benchmarks)
endmacro(ADD_BENCHARK)

ADD_BENCHARK(compression_benchmark compression_benchmark.cpp)
ADD_BENCHARK(aof_benchmark aof_benchmark.cpp)
ADD_BENCHARK(engine_benchmark engine_benchmark.cpp)

if(ENABLE_SERVER)
ADD_BENCHARK(network_benchmark network_benchmark.cpp)
TARGET_LINK_LIBRARIES(network_benchmark ${DARIADB_CLIENT_LIBS} libnet-server)
endif(ENABLE_SERVER)

macro(ADD_PERF_TEST name)
   MESSAGE(STATUS " + engine_benchmark "  ${name} " "  ${ARGN})
   add_test(${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/engine_benchmark --read-benchmark-runs=10  ${ARGN})
endmacro()

macro(ADD_NT_PERF_TEST name)
   MESSAGE(STATUS " + network_benchmark "  ${name} " "  ${ARGN})
   add_test(${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/network_benchmark --io-threads=5  ${ARGN})
endmacro()

if(ENABLE_INTEGRATION_TESTS)
   MESSAGE(STATUS "Integration tests: ")
   ADD_PERF_TEST(FAST-WRITE --strategy=FAST_WRITE)
   ADD_PERF_TEST(COMPRESSED --strategy=COMPRESSED)
  if(ENABLE_SERVER)
      ADD_NT_PERF_TEST(ONE_CLIENT  --clients-count=1)
      ADD_NT_PERF_TEST(TEN_CLIENTS  --clients-count=10)
   endif(ENABLE_SERVER)
endif(ENABLE_INTEGRATION_TESTS)
