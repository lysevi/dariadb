INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})

add_library(libcommon_test STATIC test_common.cpp test_common.h)
TARGET_LINK_LIBRARIES(libcommon_test dariadb ${Boost_LIBRARIES})
set_target_properties(libcommon_test PROPERTIES FOLDER Tests)

MESSAGE(STATUS "Test: " ${name})

macro(TEST_CASE name src)
	MESSAGE(STATUS " +" ${name})
	add_executable(${name} ${src})
    
	TARGET_LINK_LIBRARIES(${name} libcommon_test ${DARIADB_STORAGE_LIBS})
	
	add_test(${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${name}) 
	set_target_properties(${name} PROPERTIES FOLDER Tests)
endmacro(TEST_CASE)

if(ENABLE_SERVER)
   TEST_CASE(net_test net_test.cpp)
   TARGET_LINK_LIBRARIES(net_test ${DARIADB_CLIENT_LIBS} dariadb-server)
endif()

TEST_CASE(storage_common_test storage_common_test.cpp)
TEST_CASE(scheme_test scheme_test.cpp)
TEST_CASE(engine_test engine_test.cpp)
TEST_CASE(wal_test wal_test.cpp)
TEST_CASE(pages_test pages_test.cpp)
TEST_CASE(compression_test compression_test.cpp)
TEST_CASE(utils_test utils_test.cpp)
TEST_CASE(memstorage_test memstorage_test.cpp)