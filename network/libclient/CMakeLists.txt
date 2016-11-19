include(GenerateExportHeader)

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB GLOB_SRC "*.cpp")

SET(SRC ${GLOB_HDRS} ${GLOB_SRC})

add_library(dariadb-client SHARED ${SRC})
TARGET_LINK_LIBRARIES(dariadb-client dariadb dariadb-net ${Boost_LIBRARIES})

GENERATE_EXPORT_HEADER(dariadb-client
    BASE_NAME dariadb-client
    EXPORT_MACRO_NAME CL_EXPORT
    EXPORT_FILE_NAME net_cl_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)

IF(WIN32)
 target_link_libraries(dariadb-client wsock32 ws2_32)
endif(WIN32)

SET(DARIADB_CLIENT_LIBS dariadb-client ${Boost_LIBRARIES} CACHE STRING "Dariadb libs for build client.")
