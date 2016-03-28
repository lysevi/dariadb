include_directories(../libdariadb)

add_library(libcommon_test STATIC test_common.cpp test_common.h)

MESSAGE(STATUS "Test: " ${name})

macro(TEST_CASE name src)
	MESSAGE(STATUS " +" ${name})
	add_executable(${name} ${src})
	TARGET_LINK_LIBRARIES(${name} libcommon_test libdariadb ${Boost_LIBRARIES})
	add_test(${name} ${name})
endmacro(TEST_CASE)

TEST_CASE(utils_test utils_test.cpp)
TEST_CASE(compression_test compression_test.cpp)
TEST_CASE(storage_test storage_test.cpp)
TEST_CASE(capacitor_test capacitor_test.cpp)
TEST_CASE(statistic_test statistic_test.cpp)
TEST_CASE(pages_test pages_test.cpp)
TEST_CASE(union_storage_test union_storage_test.cpp)