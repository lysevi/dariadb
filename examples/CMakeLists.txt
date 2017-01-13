INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})

LINK_DIRECTORIES(${DARIADB_LIB_PATH})

macro(EXAMPLE name src)
	MESSAGE(STATUS " +" ${name})
	add_executable(example-${name} ${src})
	TARGET_LINK_LIBRARIES(example-${name} ${DARIADB_STORAGE_LIBS})
	set_target_properties(example-${name} PROPERTIES FOLDER Examples)
endmacro(EXAMPLE)


EXAMPLE(embedded embedded.cpp)
EXAMPLE(network network.cpp)
target_link_libraries(example-network ${DARIADB_STORAGE_LIBS} ${DARIADB_CLIENT_LIBS})

