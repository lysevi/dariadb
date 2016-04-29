include_directories(..)
include_directories(..)

FILE(GLOB HDRS "*.h" "*.hpp"
               "compression/*.h" "compression/*.hpp"
               "storage/*.h" "storage/*.hpp"
               "math/*.h" "math/*.hpp"
               "utils/*.h" "utils/*.hpp")

FILE(GLOB SRC "*.cpp"
              "compression/*.cpp"
              "utils/*.cpp"
              "math/*.cpp"
              "storage/*.cpp" ${HDRS})

if(MSVC)
        add_library(libdariadb STATIC ${SRC})
else(MSVC)
        add_library(libdariadb SHARED ${SRC})
        TARGET_LINK_LIBRARIES(libdariadb ${Boost_LIBRARIES})
endif(MSVC)

export(PACKAGE libdariadb)

set_target_properties(libdariadb PROPERTIES PUBLIC_HEADER "${HDRS}")

install(TARGETS libdariadb
  EXPORT dariadbTargets
  RUNTIME DESTINATION "${INSTALL_BIN_DIR}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}/static"
  PUBLIC_HEADER DESTINATION "${INSTALL_INCLUDE_DIR}/dariadb"
  COMPONENT dev)
