INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIR})

MESSAGE(STATUS "Unittests")

FILE(GLOB HDRS "*.h")
FILE(GLOB SRC "*.cpp")

add_executable(unittests ${HDRS} ${SRC})
TARGET_LINK_LIBRARIES(unittests ${DARIADB_STORAGE_LIBS} gtest_main)
add_dependencies(unittests gtest_main )
add_test(unittests ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittests) 
set_target_properties(unittests PROPERTIES FOLDER Tests)
