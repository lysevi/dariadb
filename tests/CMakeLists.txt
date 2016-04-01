include_directories(../libdariadb)

add_library(libcommon_test STATIC test_common.cpp test_common.h)
TARGET_LINK_LIBRARIES(libcommon_test libdariadb ${Boost_LIBRARIES})
set_target_properties(libcommon_test PROPERTIES FOLDER Tests)

MESSAGE(STATUS "Test: " ${name})

macro(TEST_CASE name src)
	MESSAGE(STATUS " +" ${name})
	add_executable(${name} ${src})
	TARGET_LINK_LIBRARIES(${name} libcommon_test libdariadb ${Boost_LIBRARIES})
	add_test(${name} ${name})

    set_target_properties(${name} PROPERTIES FOLDER Tests)
endmacro(TEST_CASE)

TEST_CASE(utils_test utils_test.cpp)
TEST_CASE(compression_test compression_test.cpp)
TEST_CASE(memstorage_test memstorage_test.cpp)
TEST_CASE(capacitor_test capacitor_test.cpp)
TEST_CASE(statistic_test statistic_test.cpp)
TEST_CASE(pages_test pages_test.cpp)
TEST_CASE(union_storage_test union_storage_test.cpp)
