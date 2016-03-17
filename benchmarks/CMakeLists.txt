include_directories(../libmemseries)
MESSAGE(STATUS "Benchmarks: ")

macro(ADD_BENCHARK name src)
        MESSAGE(STATUS " +" ${name})
        add_executable(${name} ${src})

        target_link_libraries(${name}
                                ${Boost_LIBRARIES}
                                libmemseries )
endmacro(ADD_BENCHARK)

ADD_BENCHARK(compression_bench compression_bench.cpp)
ADD_BENCHARK(storage_bench storage_bench.cpp)
ADD_BENCHARK(capacitor_bench capacitor_bench.cpp)
ADD_BENCHARK(integrall_bench integrall_bench.cpp)
