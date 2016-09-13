FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC})
if(MSVC)
        add_library(libnet-server STATIC ${SRC})
else(MSVC)
        add_library(libnet-server SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libnet-server libdariadb libnet-common ${Boost_LIBRARIES})
endif(MSVC)

IF(WIN32)
 target_link_libraries(libnet-server wsock32 ws2_32)
endif(WIN32)
