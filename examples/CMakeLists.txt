INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})

LINK_DIRECTORIES(${DARIADB_LIB_PATH})

MESSAGE(STATUS "Examples")
macro(EXAMPLE name src)
        MESSAGE(STATUS "+ example-${name}")
	add_executable(example-${name} ${src})
	TARGET_LINK_LIBRARIES(example-${name} ${DARIADB_STORAGE_LIBS})
	set_target_properties(example-${name} PROPERTIES FOLDER Examples)

        add_test(example-${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/example-${name})
endmacro(EXAMPLE)


EXAMPLE(embedded embedded.cpp)
EXAMPLE(network network.cpp)
target_link_libraries(example-network ${DARIADB_STORAGE_LIBS} ${DARIADB_CLIENT_LIBS})

