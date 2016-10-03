configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/config.h.cmake ${CMAKE_CURRENT_SOURCE_DIR}/config.h)
include_directories(..)

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB COMPRESSION_HDRS "compression/*.h")
FILE(GLOB COMPRESSION_2_HDRS "compression/v2/*.h")
FILE(GLOB STORE_HDRS "storage/*.h")
FILE(GLOB UTILS_HDRS "utils/*.h")
FILE(GLOB INTERFACES_HDRS "interfaces/*.h")

FILE(GLOB GLOB_SRC "*.cpp")
FILE(GLOB COMPRESSION_SRC "compression/*.cpp")
FILE(GLOB COMPRESSION_2_SRC "compression/v2/*.cpp")
FILE(GLOB STORE_SRC "storage/*.cpp")
FILE(GLOB UTILS_SRC "utils/*.cpp")
FILE(GLOB INTERFACES_SRC "interfaces/*.cpp")

source_group(compression FILES ${COMPRESSION_HDRS} ${COMPRESSION_SRC})
source_group(compression\\v2 FILES ${COMPRESSION_2_HDRS} ${COMPRESSION_2_SRC})
source_group(storage FILES ${STORE_HDRS} ${STORE_SRC})
source_group(utils FILES ${UTILS_HDRS} ${UTILS_SRC})
source_group(dariadb FILES ${GLOB_HDRS} ${GLOB_SRC})
source_group(interfaces FILES ${INTERFACES_SRC} ${INTERFACES_HDRS})

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} 
	 ${COMPRESSION_HDRS} ${COMPRESSION_SRC}  
	 ${COMPRESSION_2_HDRS} ${COMPRESSION_2_SRC}  
	 ${STORE_HDRS} ${STORE_SRC} 
	 ${UTILS_HDRS}  ${UTILS_SRC} 
         ${INTERFACES_HDRS} ${INTERFACES_SRC})

SET(DARIADB_STORAGE_LIBS libdariadb ${Boost_LIBRARIES} CACHE STRING "Dariadb libs for build storage.")

if(MSVC)
        add_library(libdariadb STATIC ${SRC})
else(MSVC)
        add_library(libdariadb SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libdariadb ${Boost_LIBRARIES})
endif(MSVC)

IF(WIN32)
 target_link_libraries(libdariadb wsock32 ws2_32)
endif(WIN32)
