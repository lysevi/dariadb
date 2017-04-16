INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIR})

MESSAGE(STATUS "Microbenchmarks")

FILE(GLOB HDRS "*.h")
FILE(GLOB SRC "*.cpp")

add_executable(microbenchmarks ${HDRS} ${SRC})
TARGET_LINK_LIBRARIES(microbenchmarks ${DARIADB_STORAGE_LIBS} ${DARIADB_CLIENT_LIBS} dariadb-server benchmark)
target_link_libraries(microbenchmarks benchmark)
set_target_properties(microbenchmarks PROPERTIES FOLDER Benchmarks)
add_test(microbenchmarks ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/microbenchmarks) 