include_directories(../libmemseries)

add_library(libcommon_test STATIC test_common.cpp test_common.h)

MESSAGE(STATUS "Test: " ${name})

macro(TEST_CASE name src)
MESSAGE(STATUS " +" ${name})
add_executable(${name} ${src})
TARGET_LINK_LIBRARIES(${name} libcommon_test libmemseries ${Boost_LIBRARIES})
add_test(${name} ${name})
endmacro(TEST_CASE)

TEST_CASE(utils_test utils_test.cpp)
TEST_CASE(compression_test compression_test.cpp)
TEST_CASE(storage_test storage_test.cpp)
TEST_CASE(net_test net_test.cpp)
