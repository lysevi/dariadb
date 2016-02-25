include_directories(..)
include_directories(..)

FILE(GLOB HDRS "*.h" "*.hpp")
FILE(GLOB SRC "*.cpp" ${HDRS})

if(MSVC)
        add_library(libmemseries STATIC ${SRC})
else(MSVC)
	add_library(libmemseries SHARED ${SRC})
	TARGET_LINK_LIBRARIES(libmemseries ${Boost_LIBRARIES})
endif(MSVC)

export(PACKAGE libmemseries)

set_target_properties(libmemseries PROPERTIES PUBLIC_HEADER "${HDRS}")
 
install(TARGETS libmemseries
  EXPORT memseriesTargets
  RUNTIME DESTINATION "${INSTALL_BIN_DIR}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}/static"
  PUBLIC_HEADER DESTINATION "${INSTALL_INCLUDE_DIR}/libmemseries"
  COMPONENT dev)