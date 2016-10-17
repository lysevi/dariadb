SET(DARIADB_NETWORK_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
            CACHE STRING "dariadb storage include dir for network clients.")

INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})

MESSAGE(STATUS "Build network client/server.")

add_subdirectory (common)
add_subdirectory (libclient)
add_subdirectory (libserver)
add_subdirectory (dariadb)
