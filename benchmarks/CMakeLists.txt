include_directories(../libdariadb)
MESSAGE(STATUS "Benchmarks: ")

macro(ADD_BENCHARK name src)
        MESSAGE(STATUS " +" ${name})
        add_executable(${name} ${src} bench_common.h)

        target_link_libraries(${name}
                                ${CMAKE_THREAD_LIBS_INIT}
                                ${Boost_LIBRARIES}
                                libdariadb)

         set_target_properties(${name} PROPERTIES FOLDER Benchmarks)
endmacro(ADD_BENCHARK)

ADD_BENCHARK(compression_benchmark compression_benchmark.cpp)
ADD_BENCHARK(aof_benchmark aof_benchmark.cpp)
ADD_BENCHARK(engine_benchmark engine_benchmark.cpp)
ADD_BENCHARK(statistic_benchmark statistic_benchmark.cpp)
ADD_BENCHARK(utils_benchmark utils_benchmark.cpp)

macro(ADD_PERF_TEST name)
   MESSAGE(STATUS " + engine_benchmark.cpp "  ${name} " "  ${ARGN})
   add_test(${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/engine_benchmark --read-benchmark-runs=10  ${ARGN})
endmacro(ADD_PERF_TEST)

if(ENABLE_INTEGRATION_TESTS)
   MESSAGE(STATUS "Integration tests: ")
   ADD_PERF_TEST(DYNAMIC-TIME --strategy=DYNAMIC --store-period=3600000)
   ADD_PERF_TEST(DYNAMIC --strategy=DYNAMIC)
   ADD_PERF_TEST(FAST-READ --strategy=FAST_READ)
   ADD_PERF_TEST(FAST-WRITE --strategy=FAST_WRITE)
   ADD_PERF_TEST(COMPRESSED --strategy=COMPRESSED)
endif(ENABLE_INTEGRATION_TESTS)
