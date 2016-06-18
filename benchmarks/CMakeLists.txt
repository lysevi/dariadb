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

ADD_BENCHARK(compression_bench compression_bench.cpp)
ADD_BENCHARK(hard_benchmark hard_benchmark.cpp)
ADD_BENCHARK(aof_bench aof_bench.cpp)
ADD_BENCHARK(capacitor_bench capacitor_bench.cpp)
ADD_BENCHARK(page_benchmark page_benchmark.cpp)
ADD_BENCHARK(statistic_bench statistic_bench.cpp)
ADD_BENCHARK(perf_benchmark perf_benchmark.cpp)
ADD_BENCHARK(utils_benchmark utils_benchmark.cpp)
