configure_file(${CMAKE_SOURCE_DIR}/cmake/config.h.cmake ${CMAKE_CURRENT_SOURCE_DIR}/config.h)
include_directories(..)

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB COMPRESSION_HDRS "compression/*.h")
FILE(GLOB STORE_HDRS "storage/*.h")
FILE(GLOB MATH_HDRS  "math/*.h")
FILE(GLOB UTILS_HDRS "utils/*.h")
FILE(GLOB INTERFACES_HDRS "interfaces/*.h")

FILE(GLOB NET_HDRS "net/*.h")
FILE(GLOB NETI_HDRS "net/interfaces/*.h")
FILE(GLOB NET_SERVER_HDRS "net/server/*.h")

FILE(GLOB GLOB_SRC "*.cpp")
FILE(GLOB COMPRESSION_SRC "compression/*.cpp")
FILE(GLOB STORE_SRC "storage/*.cpp")
FILE(GLOB MATH_SRC  "math/*.cpp")
FILE(GLOB UTILS_SRC "utils/*.cpp")
FILE(GLOB INTERFACES_SRC "interfaces/*.cpp")
FILE(GLOB NET_SRC "net/*.cpp")
FILE(GLOB NETI_SRC "net/interfaces/*.cpp")
FILE(GLOB NET_SERVER_SRC "net/server/*.cpp")

source_group(compression FILES ${COMPRESSION_HDRS} ${COMPRESSION_SRC})
source_group(storage FILES ${STORE_HDRS} ${STORE_SRC})
source_group(math FILES ${MATH_HDRS} ${MATH_SRC})
source_group(utils FILES ${UTILS_HDRS} ${UTILS_SRC})
source_group(dariadb FILES ${GLOB_HDRS} ${GLOB_SRC})
source_group(interfaces FILES ${INTERFACES_SRC} ${INTERFACES_HDRS})
source_group("net" FILES ${NET_SRC} ${NET_HDRS} ${NET_HDRS})
source_group("net\\interfaces" FILES  ${NETI_HDRS} ${NETI_SRC})
source_group("net\\server" FILES ${NET_SERVER_HDRS} ${NET_SERVER_SRC})

SET(SRC  ${GLOB_HDRS} ${GLOB_SRC} 
	 ${COMPRESSION_HDRS} ${COMPRESSION_SRC}  
	 ${STORE_HDRS} ${STORE_SRC} 
	 ${MATH_HDRS}  ${MATH_SRC} 
	 ${UTILS_HDRS}  ${UTILS_SRC} 
         ${INTERFACES_HDRS} ${INTERFACES_SRC}
         ${NET_SRC} ${NET_HDRS} ${NETI_HDRS} ${NETI_SRC} ${NET_SERVER_HDRS} ${NET_SERVER_SRC})

if(MSVC)
        add_library(libdariadb STATIC ${SRC})
else(MSVC)
        add_library(libdariadb SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libdariadb ${Boost_LIBRARIES})
endif(MSVC)

IF(WIN32)
 target_link_libraries(libdariadb wsock32 ws2_32)
endif(WIN32)
