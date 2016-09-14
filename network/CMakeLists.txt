include_directories(.)
include_directories(..)

MESSAGE(STATUS "Build network client/server.")

add_subdirectory (libclient)
add_subdirectory (libserver)
add_subdirectory (dariadb)
