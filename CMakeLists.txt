cmake_minimum_required (VERSION 3.1)
set(PROJECT "dariadb")
project (${PROJECT} VERSION 0.3.0)

IF(NOT CMAKE_BUILD_TYPE)
        message(STATUS "CMAKE_BUILD_TYPE is not set, set to default = RELWITHDEBINFO")
        SET(CMAKE_BUILD_TYPE "RELWITHDEBINFO")
ENDIF()

MESSAGE( STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

option(DARIADB_ENABLE_TESTS "Enable testing of the dariadb." ON)
option(DARIADB_ENABLE_INTEGRATION_TESTS "Enable integration test." ON)
option(DARIADB_ENABLE_SERVER "Enable build dariadb server." ON)
option(DARIADB_ENABLE_BENCHMARKS "Enable build dariadb benchmarks." ON)
option(DARIADB_ENABLE_SAMPLES "Build dariadb sample programs." ON)
option(DARIADB_ENABLE_DOUBLECHECKS "More checks in code." OFF)
option(DARIADB_ASAN_UBSAN "Enable address & undefined behavior sanitizer for binary." OFF)
option(DARIADB_MSAN "Enable memory sanitizer for binary." OFF)
option(DARIADB_SYSTEM_JEMALLOC "Use jemalloc installed in the system." ON)

if((DARIADB_ASAN_UBSAN AND DARIADB_MSAN))
  message(FATAL_ERROR "Sanitizers cannot be enabled simultaneously.")
endif()

MESSAGE(STATUS "Options: ")
MESSAGE(STATUS "DARIADB_ENABLE_TESTS - " ${DARIADB_ENABLE_TESTS})
MESSAGE(STATUS "DARIADB_ENABLE_INTEGRATION_TESTS - " ${DARIADB_ENABLE_INTEGRATION_TESTS})
MESSAGE(STATUS "DARIADB_ENABLE_SERVER - " ${DARIADB_ENABLE_SERVER})
MESSAGE(STATUS "DARIADB_ENABLE_BENCHMARKS - " ${DARIADB_ENABLE_BENCHMARKS})
MESSAGE(STATUS "DARIADB_ENABLE_SAMPLES - " ${DARIADB_ENABLE_SAMPLES})
MESSAGE(STATUS "DARIADB_ENABLE_DOUBLECHECKS - " ${DARIADB_ENABLE_DOUBLECHECKS})
MESSAGE(STATUS "DARIADB_MSAN - " ${DARIADB_MSAN})
MESSAGE(STATUS "DARIADB_ASAN_UBSAN - " ${DARIADB_ASAN_UBSAN})
MESSAGE(STATUS "DARIADB_SYSTEM_JEMALLOC - " ${DARIADB_SYSTEM_JEMALLOC})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(AddCXXCompilerFlag)
include(CompillerSettings)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 

set(BOOST_DEP system filesystem date_time)

if(DARIADB_ENABLE_TESTS)
  set(BOOST_DEP ${BOOST_DEP} unit_test_framework)
endif()

IF(DARIADB_ENABLE_DOUBLECHECKS)
 add_definitions(-DDOUBLE_CHECKS)
ENDIF()

IF(WIN32 AND MSVC)
  find_package(Boost 1.54.0 COMPONENTS ${BOOST_DEP} REQUIRED)
else()
  set(BOOST_DEP ${BOOST_DEP} program_options)
  find_package(Boost  REQUIRED ${BOOST_DEP})
ENDIF()

IF(Boost_FOUND)
  INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
  LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
  SET(DARIADB_LIB_PATH ${Boost_LIBRARY_DIRS}
              CACHE STRING "dariadb libs path.")
else()
  MESSAGE(FATAL_ERROR "boost not found")
  EXIT()
ENDIF()

find_package (Threads)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

include_directories(extern)
include_directories(extern/spdlog/include)
include_directories(extern/stx-btree/include)

if(DARIADB_SYSTEM_JEMALLOC)
   include(FindJeMalloc)
   if(JEMALLOC_FOUND)
      MESSAGE(STATUS "jemalloc-dev - found")
      SET(JEMALLOC_LIBS ${JEMALLOC_LIBRARIES})
   endif(JEMALLOC_FOUND)
else(DARIADB_SYSTEM_JEMALLOC)
   MESSAGE(STATUS "LOCAL JEMALLOC")
   set(BuildTests OFF)
   add_subdirectory(extern/jemalloc-cmake)

   include_directories(extern/jemalloc-cmake/include/jemalloc)
   if(MSVC)
      include_directories(extern/jemalloc-cmake/include/msvc_compat)
   endif()
   SET(JEMALLOC_LIBS jemallocso)

   set_target_properties(jemalloc_jet PROPERTIES FOLDER Extern)
   set_target_properties(jemalloc_util_int_test PROPERTIES FOLDER Extern)
   set_target_properties(jemallocso PROPERTIES FOLDER Extern)
   set_target_properties(jemalloc PROPERTIES FOLDER Extern)
endif(DARIADB_SYSTEM_JEMALLOC)

add_subdirectory(extern/libsqlite3)
include_directories(extern/libsqlite3)

set_target_properties(sqlite3 PROPERTIES FOLDER Extern)

SET(DARIADB_STORAGE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
            CACHE STRING "dariadb storage include dir for storage engine.")

add_subdirectory (libdariadb)

if(DARIADB_ENABLE_SERVER)
  add_subdirectory (network)
endif()

if (DARIADB_ENABLE_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(DARIADB_ENABLE_BENCHMARKS)
  add_subdirectory (benchmarks)
endif()

add_subdirectory (tools)

if(DARIADB_ENABLE_SAMPLES)
  add_subdirectory(examples)
endif()
