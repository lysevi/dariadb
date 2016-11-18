include(GenerateExportHeader)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/config.h.cmake ${CMAKE_CURRENT_SOURCE_DIR}/config.h)
INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB COMPRESSION_HDRS "compression/*.h")
FILE(GLOB COMPRESSION_2_HDRS "compression/v2/*.h")
FILE(GLOB STORE_HDRS "storage/*.h")
FILE(GLOB UTILS_HDRS "utils/*.h")
FILE(GLOB INTERFACES_HDRS "interfaces/*.h")
FILE(GLOB ADS_HDRS "ads/*.h")

FILE(GLOB GLOB_SRC "*.cpp")
FILE(GLOB COMPRESSION_SRC "compression/*.cpp")
FILE(GLOB COMPRESSION_2_SRC "compression/v2/*.cpp")
FILE(GLOB STORE_SRC "storage/*.cpp")
FILE(GLOB UTILS_SRC "utils/*.cpp")
FILE(GLOB INTERFACES_SRC "interfaces/*.cpp")
FILE(GLOB ADS_SRC "ads/*.cpp")

source_group(compression FILES ${COMPRESSION_HDRS} ${COMPRESSION_SRC})
source_group(compression\\v2 FILES ${COMPRESSION_2_HDRS} ${COMPRESSION_2_SRC})
source_group(storage FILES ${STORE_HDRS} ${STORE_SRC})
source_group(utils FILES ${UTILS_HDRS} ${UTILS_SRC})
source_group(dariadb FILES ${GLOB_HDRS} ${GLOB_SRC})
source_group(interfaces FILES ${INTERFACES_SRC} ${INTERFACES_HDRS})
source_group(ads FILES ${ADS_SRC} ${ADS_HDRS})

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} 
	 ${COMPRESSION_HDRS} ${COMPRESSION_SRC}  
	 ${COMPRESSION_2_HDRS} ${COMPRESSION_2_SRC}  
	 ${STORE_HDRS} ${STORE_SRC} 
	 ${UTILS_HDRS}  ${UTILS_SRC} 
         ${INTERFACES_HDRS} ${INTERFACES_SRC}
	 ${ADS_SRC} ${ADS_HDRS})

SET(DARIADB_STORAGE_LIBS libdariadb ${Boost_LIBRARIES} CACHE STRING "Dariadb libs for build storage.")

add_library(libdariadb SHARED ${SRC})
TARGET_LINK_LIBRARIES(libdariadb ${Boost_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

IF(WIN32)
 target_link_libraries(libdariadb wsock32 ws2_32)
endif()

GENERATE_EXPORT_HEADER(libdariadb
    BASE_NAME libdariadb
    EXPORT_MACRO_NAME EXPORT
    EXPORT_FILE_NAME st_exports.h
    STATIC_DEFINE SHARED_EXPORTS_BUILT_AS_STATIC)
