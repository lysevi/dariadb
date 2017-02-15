include(GenerateExportHeader)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/config.h.cmake ${CMAKE_CURRENT_SOURCE_DIR}/config.h)
INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB COMPRESSION_HDRS "compression/*.h")
FILE(GLOB STORE_HDRS "storage/*.h")
FILE(GLOB MSTORE_HDRS "storage/memstorage/*.h")
FILE(GLOB PSTORE_HDRS "storage/pages/*.h")
FILE(GLOB WSTORE_HDRS "storage/wal/*.h")
FILE(GLOB SCHEME_HDRS "scheme/*.h")


FILE(GLOB UTILS_HDRS "utils/*.h")
FILE(GLOB UTILS_ASYNC_HDRS "utils/async/*.h")
FILE(GLOB INTERFACES_HDRS "interfaces/*.h")

FILE(GLOB GLOB_SRC "*.cpp")
FILE(GLOB COMPRESSION_SRC "compression/*.cpp")
FILE(GLOB STORE_SRC "storage/*.cpp")
FILE(GLOB MSTORE_SRC "storage/memstorage/*.cpp")
FILE(GLOB PSTORE_SRC "storage/pages/*.cpp")
FILE(GLOB WSTORE_SRC "storage/wal/*.cpp")
FILE(GLOB UTILS_SRC "utils/*.cpp")
FILE(GLOB UTILS_ASYNC_SRC "utils/async/*.cpp")
FILE(GLOB INTERFACES_SRC "interfaces/*.cpp")
FILE(GLOB SCHEME_SRC "scheme/*.cpp")

source_group(compression FILES ${COMPRESSION_HDRS} ${COMPRESSION_SRC})
source_group(storage FILES ${STORE_HDRS} ${STORE_SRC})
source_group(storage\\memstorage FILES ${MSTORE_HDRS} ${MSTORE_SRC})
source_group(storage\\pages FILES ${PSTORE_HDRS} ${PSTORE_SRC})
source_group(storage\\wal FILES ${WSTORE_HDRS} ${WSTORE_SRC})
source_group(utils FILES ${UTILS_HDRS} ${UTILS_SRC})
source_group(utils\\async FILES ${UTILS_ASYNC_HDRS} ${UTILS_ASYNC_SRC})
source_group(engine FILES ${GLOB_HDRS} ${GLOB_SRC})
source_group(interfaces FILES ${INTERFACES_SRC} ${INTERFACES_HDRS})
source_group(scheme FILES ${SCHEME_SRC} ${SCHEME_HDRS})

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} 
	 ${COMPRESSION_HDRS} ${COMPRESSION_SRC}  
	 ${STORE_HDRS} ${STORE_SRC} 
	 ${MSTORE_HDRS} ${MSTORE_SRC} 
	 ${PSTORE_HDRS} ${PSTORE_SRC} 
	 ${WSTORE_HDRS} ${WSTORE_SRC} 
	 ${UTILS_HDRS}  ${UTILS_SRC} 
	 ${UTILS_ASYNC_HDRS}  ${UTILS_ASYNC_SRC} 
         ${INTERFACES_HDRS} ${INTERFACES_SRC}
	 ${SCHEME_SRC} ${SCHEME_HDRS})

SET(DARIADB_STORAGE_LIBS dariadb ${Boost_LIBRARIES} sqlite3 CACHE STRING "Dariadb libs for build storage.")

add_library(dariadb SHARED ${SRC})
TARGET_LINK_LIBRARIES(dariadb ${Boost_LIBRARIES} sqlite3 ${CMAKE_THREAD_LIBS_INIT})

IF(WIN32)
 target_link_libraries(dariadb wsock32 ws2_32)
endif()

GENERATE_EXPORT_HEADER(dariadb
    BASE_NAME dariadb
    EXPORT_MACRO_NAME EXPORT
    EXPORT_FILE_NAME st_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)
