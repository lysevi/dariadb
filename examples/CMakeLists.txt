INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})

LINK_DIRECTORIES(${DARIADB_LIB_PATH})

MESSAGE(STATUS "Examples")
macro(EXAMPLE name src)
        MESSAGE(STATUS "+ example-${name}")
	add_executable(example-${name} ${src})
	TARGET_LINK_LIBRARIES(example-${name} ${DARIADB_STORAGE_LIBS})
	set_target_properties(example-${name} PROPERTIES FOLDER Examples)
endmacro(EXAMPLE)


EXAMPLE(embedded embedded.cpp)
add_test(example-embedded ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/example-embedded)

EXAMPLE(memory-only memory-only.cpp)
add_test(example-memory-only ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/example-memory-only)

EXAMPLE(embedded-create embedded-create.cpp)
EXAMPLE(embedded-interval embedded-interval.cpp)
EXAMPLE(embedded-timepoint embedded-timepoint.cpp)
EXAMPLE(embedded-statistic embedded-statistic.cpp)

EXAMPLE(shard-embedded shard_embedded.cpp)
add_test(example-embedded ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/example-shard-embedded)

EXAMPLE(network network.cpp)
target_link_libraries(example-network ${DARIADB_STORAGE_LIBS} ${DARIADB_CLIENT_LIBS})

