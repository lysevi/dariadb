include_directories(../libtimedb)

add_library(timedb_test STATIC test_common.cpp test_common.h)

MESSAGE(STATUS "Test: " ${name})

macro(TEST_CASE name src)
MESSAGE(STATUS " +" ${name})
add_executable(${name} ${src})
TARGET_LINK_LIBRARIES(${name} timedb_test libtimedb ${Boost_LIBRARIES})
add_test(${name} ${name})
endmacro(TEST_CASE)

TEST_CASE(utils_test utils_test.cpp)
TEST_CASE(compression_test compression_test.cpp)
