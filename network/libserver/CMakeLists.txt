FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} ${CMN_HDRS} ${CMN_SRC})

if(MSVC)
        add_library(libnet-server STATIC ${SRC})
else()
        add_library(libnet-server SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libnet-server libdariadb ${Boost_LIBRARIES}  libcommon-net)
endif()

if(SYSTEM_PROTOBUF) #Legacy mode
    target_link_libraries(libnet-server ${PROTOBUF_LIBRARIES})
else()
    target_link_libraries(libnet-server protobuf::libprotobuf)
endif()

IF(WIN32)
 target_link_libraries(libnet-server wsock32 ws2_32)
endif(WIN32)
