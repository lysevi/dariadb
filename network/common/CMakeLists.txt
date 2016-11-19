include(GenerateExportHeader)

FILE(GLOB CMN_HDRS "*.h")
FILE(GLOB CMN_SRC "*.cpp")

SET(SRC ${CMN_HDRS} ${CMN_SRC})

add_library(dariadb-net SHARED ${SRC})

IF(WIN32)
 target_link_libraries(dariadb-net wsock32 ws2_32)
endif(WIN32)
 
target_link_libraries(dariadb-net dariadb)

GENERATE_EXPORT_HEADER(dariadb-net
    BASE_NAME dariadb-net
    EXPORT_MACRO_NAME CM_EXPORT
    EXPORT_FILE_NAME net_cmn_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)
