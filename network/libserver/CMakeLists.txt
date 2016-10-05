FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

FILE(GLOB CMN_HDRS "../common/*.h")
FILE(GLOB CMN_SRC "../common/*.cpp")

source_group(common FILES ${CMN_HDRS} ${CMN_SRC})

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} ${CMN_HDRS} ${CMN_SRC})

if(MSVC)
        add_library(libnet-server STATIC ${SRC})
else()
        add_library(libnet-server SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libnet-server libdariadb ${Boost_LIBRARIES})
endif()

IF(WIN32)
 target_link_libraries(libnet-server wsock32 ws2_32)
endif(WIN32)
