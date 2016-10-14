if(SYSTEM_PROTOBUF)
  find_package(Protobuf REQUIRED)
else()
 find_package(protobuf CONFIG REQUIRED)
endif()


message(STATUS "Using Protocol Buffers ${Protobuf_VERSION}")
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)

# http://www.cmake.org/Wiki/CMake_FAQ#How_can_I_build_my_MSVC_application_with_a_static_runtime.3F
if(MSVC AND protobuf_MSVC_STATIC_RUNTIME)
foreach(flag_var
     CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE
     CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
   if(${flag_var} MATCHES "/MD")
     string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
   endif(${flag_var} MATCHES "/MD")
 endforeach()
endif()

SET(DARIADB_NETWORK_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} ${PROTOBUF_INCLUDE_DIRS}
            CACHE STRING "dariadb storage include dir for network clients.")

INCLUDE_DIRECTORIES(${DARIADB_NETWORK_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${DARIADB_STORAGE_INCLUDE_DIR})

MESSAGE(STATUS "Build network client/server.")

add_subdirectory (libclient)
add_subdirectory (libserver)
add_subdirectory (dariadb)
