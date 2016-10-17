include(GenerateExportHeader)
FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC})

add_library(libnet-server SHARED ${SRC})
TARGET_LINK_LIBRARIES(libnet-server libdariadb libcommon-net ${Boost_LIBRARIES})

IF(WIN32)
 target_link_libraries(libnet-server wsock32 ws2_32)
endif(WIN32)

GENERATE_EXPORT_HEADER(libnet-server
    BASE_NAME libnet-server
    EXPORT_MACRO_NAME DARIADBNET_SERVER_EXPORTS
    EXPORT_FILE_NAME dariadb_net_srv_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)