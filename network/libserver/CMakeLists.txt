include(GenerateExportHeader)
FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

FILE(GLOB HTTP_HDRS "http/*.h")
FILE(GLOB HTTP_SRC "http/*.cpp")

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} ${HTTP_HDRS} ${HTTP_SRC})

source_group(http FILES ${HTTP_HDRS} ${HTTP_SRC})

add_library(dariadb-server SHARED ${SRC})
TARGET_LINK_LIBRARIES(dariadb-server dariadb dariadb-net ${Boost_LIBRARIES})

IF(WIN32)
 target_link_libraries(dariadb-server wsock32 ws2_32)
endif(WIN32)

GENERATE_EXPORT_HEADER(dariadb-server
    BASE_NAME dariadb-server
    EXPORT_MACRO_NAME SRV_EXPORT
    EXPORT_FILE_NAME net_srv_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)
