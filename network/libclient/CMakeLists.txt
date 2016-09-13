FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

FILE(GLOB CMN_HDRS "../common/*.h")
FILE(GLOB CMN_SRC "../common/*.cpp")

source_group(common FILES ${CMN_HDRS} ${CMN_SRC})

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} ${CMN_HDRS} ${CMN_SRC})

add_library(libdariadb-cl SHARED ${SRC})
TARGET_LINK_LIBRARIES(libdariadb-cl libdariadb ${Boost_LIBRARIES})

IF(WIN32)
 target_link_libraries(libdariadb-cl wsock32 ws2_32)
endif(WIN32)
