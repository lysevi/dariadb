include_directories(..)

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

add_executable(dariadb ${GLOB_SRC} ${GLOB_HDRS})
TARGET_LINK_LIBRARIES(dariadb libdariadb libnet-server ${CMAKE_THREAD_LIBS_INIT} ${Boost_LIBRARIES})