include_directories(..)
include_directories(..)

FILE(GLOB GLOB_HDRS "*.h")
FILE(GLOB COMPRESSION_HDRS "compression/*.h")
FILE(GLOB STORE_HDRS "storage/*.h")
FILE(GLOB MATH_HDRS  "math/*.h")
FILE(GLOB UTILS_HDRS "utils/*.h")

FILE(GLOB GLOB_SRC "*.cpp")
FILE(GLOB COMPRESSION_SRC "compression/*.cpp")
FILE(GLOB STORE_SRC "storage/*.cpp")
FILE(GLOB MATH_SRC  "math/*.cpp")
FILE(GLOB UTILS_SRC "utils/*.cpp")

source_group(compression FILES ${COMPRESSION_HDRS} ${COMPRESSION_SRC})
source_group(storage FILES ${STORE_HDRS} ${STORE_SRC})
source_group(math FILES ${MATH_HDRS} ${MATH_SRC})
source_group(utils FILES ${UTILS_HDRS} ${UTILS_SRC})
source_group(dariadb FILES ${GLOB_HDRS} ${GLOB_SRC})

SET(SRC  ${GLOB_HDRS} ${COMPRESSION_HDRS} ${STORE_HDRS} ${MATH_HDRS} ${UTILS_HDRS}
	 ${GLOB_SRC} ${COMPRESSION_SRC} ${STORE_SRC} ${MATH_SRC} ${UTILS_SRC})

if(MSVC)
        add_library(libdariadb STATIC ${SRC})
else(MSVC)
        add_library(libdariadb SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libdariadb ${Boost_LIBRARIES})
endif(MSVC)

