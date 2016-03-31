include_directories(../libdariadb)
MESSAGE(STATUS "Benchmarks: ")

macro(ADD_BENCHARK name src)
        MESSAGE(STATUS " +" ${name})
        add_executable(${name} ${src})

        target_link_libraries(${name}
                                ${Boost_LIBRARIES}
                                libdariadb)

         set_target_properties(${name} PROPERTIES FOLDER Benchmarks)
endmacro(ADD_BENCHARK)

ADD_BENCHARK(compression_bench compression_bench.cpp)
ADD_BENCHARK(memstorage_bench memstorage_bench.cpp)
ADD_BENCHARK(memstorage_hard_benchmark memstorage_hard_benchmark.cpp)
ADD_BENCHARK(capacitor_bench capacitor_bench.cpp)
ADD_BENCHARK(thread_bench thread_bench.cpp)
ADD_BENCHARK(page_benchmark page_benchmark.cpp)
ADD_BENCHARK(statistic_bench statistic_bench.cpp)
ADD_BENCHARK(perf_benchmark perf_benchmark.cpp)
