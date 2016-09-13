FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC})

if(MSVC)
        add_library(libnet-client STATIC ${SRC})
else(MSVC)
        add_library(libnet-client SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libnet-client libnet-common ${Boost_LIBRARIES})
endif(MSVC)

IF(WIN32)
 target_link_libraries(libnet-client wsock32 ws2_32 libnet-common)
endif(WIN32)
