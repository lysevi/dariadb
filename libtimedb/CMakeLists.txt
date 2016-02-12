include_directories(..)
include_directories(..)

FILE(GLOB HDRS "*.h" "*.hpp")
FILE(GLOB SRC "*.cpp" ${HDRS})

if(MSVC)
        add_library(libtimedb STATIC ${SRC})
else(MSVC)
	add_library(libtimedb SHARED ${SRC})
	TARGET_LINK_LIBRARIES(libtimedb ${Boost_LIBRARIES})
endif(MSVC)

export(PACKAGE libtimedb)

set_target_properties(libtimedb PROPERTIES PUBLIC_HEADER "${HDRS}")
 
install(TARGETS libtimedb
  EXPORT timedbTargets
  RUNTIME DESTINATION "${INSTALL_BIN_DIR}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}/static"
  PUBLIC_HEADER DESTINATION "${INSTALL_INCLUDE_DIR}/libtimedb"
  COMPONENT dev)