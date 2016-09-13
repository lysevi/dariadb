include_directories(..)
include_directories(../network)

add_library(libcommon_test STATIC test_common.cpp test_common.h)
TARGET_LINK_LIBRARIES(libcommon_test libdariadb ${Boost_LIBRARIES})
set_target_properties(libcommon_test PROPERTIES FOLDER Tests)

MESSAGE(STATUS "Test: " ${name})

macro(TEST_CASE name src)
	MESSAGE(STATUS " +" ${name} ": " "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${name}")
	add_executable(${name} ${src})
        TARGET_LINK_LIBRARIES(${name} libcommon_test libdariadb ${CMAKE_THREAD_LIBS_INIT} ${Boost_LIBRARIES})
	add_test(${name} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${name})
        set_target_properties(${name} PROPERTIES FOLDER Tests)
endmacro(TEST_CASE)

TEST_CASE(net_test net_test.cpp)
TARGET_LINK_LIBRARIES(net_test libnet-client libnet-server)

TEST_CASE(statistic_test statistic_test.cpp)
TEST_CASE(engine_test engine_test.cpp)
TEST_CASE(aof_test aof_test.cpp)
TEST_CASE(capacitor_test capacitor_test.cpp)
TEST_CASE(pages_test pages_test.cpp)
TEST_CASE(compression_test compression_test.cpp)
TEST_CASE(utils_test utils_test.cpp)
