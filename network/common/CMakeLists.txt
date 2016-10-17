include(GenerateExportHeader)

FILE(GLOB CMN_HDRS "*.h")
FILE(GLOB CMN_SRC "*.cpp")

SET(SRC ${CMN_HDRS} ${CMN_SRC})

add_library(libcommon-net SHARED ${SRC})

IF(WIN32)
 target_link_libraries(libcommon-net wsock32 ws2_32)
endif(WIN32)
 
target_link_libraries(libcommon-net libdariadb)

GENERATE_EXPORT_HEADER(libcommon-net
    BASE_NAME libcommon-net
    EXPORT_MACRO_NAME DARIADBNET_CMN_EXPORTS
    EXPORT_FILE_NAME dariadb_net_cmn_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)