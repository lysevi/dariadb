include(GenerateExportHeader)

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

SET(SRC ${GLOB_HDRS} ${GLOB_SRC})

add_library(libdariadb-cl SHARED ${SRC})
TARGET_LINK_LIBRARIES(libdariadb-cl libdariadb libcommon-net ${Boost_LIBRARIES})

GENERATE_EXPORT_HEADER(libdariadb-cl           
    BASE_NAME libdariadb-cl
    EXPORT_MACRO_NAME DARIADBNET_CL_EXPORTS
    EXPORT_FILE_NAME dariadb_cl_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)

IF(WIN32)
 target_link_libraries(libdariadb-cl wsock32 ws2_32)
endif(WIN32)

SET(DARIADB_CLIENT_LIBS libdariadb-cl ${Boost_LIBRARIES} CACHE STRING "Dariadb libs for build client.")
