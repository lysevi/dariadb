FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

FILE(GLOB CMN_HDRS "../libnet-common/*.h")
FILE(GLOB CMN_SRC "../libnet-common/*.cpp")

source_group(common FILES ${CMN_HDRS} ${CMN_SRC})

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} ${CMN_HDRS} ${CMN_SRC})

if(MSVC)
        add_library(libnet-client STATIC ${SRC})
else(MSVC)
        add_library(libnet-client SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libnet-client ${Boost_LIBRARIES})
endif(MSVC)

IF(WIN32)
 target_link_libraries(libnet-client wsock32 ws2_32)
endif(WIN32)
