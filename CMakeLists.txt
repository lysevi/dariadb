cmake_minimum_required (VERSION 3.1)
set(PROJECT "dariadb")
project (${PROJECT} VERSION 0.1.1)

option(ENABLE_TESTS "Enable testing of the dariadb." ON)
option(ENABLE_METRICS "Enable code metrics." ON)
option(ENABLE_INTEGRATION_TESTS "Enable integration test." ON)
option(ENABLE_SERVER "Enable build dariadb server." ON)
option(ENABLE_BENCHMARKS "Enable build dariadb benchmarks." ON)
option(CLANG_ASAN_UBSAN "Enable Clang address & undefined behavior sanitizer for binary." OFF)
option(CLANG_MSAN "Enable Clang memory sanitizer for binary." OFF)
option(SYSTEM_PROTOBUF "use system protobuf." ON)

if((CLANG_ASAN_UBSAN AND CLANG_MSAN))
  message(FATAL_ERROR "Sanitizers cannot be enabled simultaneously.")
endif()

if((CLANG_ASAN_UBSAN OR CLANG_MSAN) AND NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR "Sanitizers are only supported for Clang.")
  EXIT()
endif()

MESSAGE(STATUS "Options: ")
MESSAGE(STATUS "ENABLE_TESTS - " ${ENABLE_TESTS})
MESSAGE(STATUS "ENABLE_METRICS - " ${ENABLE_METRICS})
MESSAGE(STATUS "ENABLE_INTEGRATION_TESTS - " ${ENABLE_INTEGRATION_TESTS})
MESSAGE(STATUS "ENABLE_SERVER - " ${ENABLE_SERVER})
MESSAGE(STATUS "ENABLE_BENCHMARKS - " ${ENABLE_BENCHMARKS})
MESSAGE(STATUS "CLANG_MSAN - " ${CLANG_MSAN})
MESSAGE(STATUS "CLANG_ASAN_UBSAN - " ${CLANG_ASAN_UBSAN})
MESSAGE(STATUS "SYSTEM_PROTOBUF - " ${SYSTEM_PROTOBUF})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(AddCXXCompilerFlag)
include(CompillerSettings)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(BOOST_DEP system filesystem system date_time)

if(ENABLE_TESTS)
  set(BOOST_DEP ${BOOST_DEP} unit_test_framework)
endif()

IF(WIN32 AND MSVC)
  find_package(Boost 1.57.0 COMPONENTS ${BOOST_DEP} REQUIRED)
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

if (ENABLE_METRICS)
  add_definitions(-DENABLE_METRICS)
endif()

include_directories(extern)
include_directories(extern/spdlog/include)
include_directories(extern/stx-btree/include)

SET(DARIADB_STORAGE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
            CACHE STRING "dariadb storage include dir for storage engine.")

add_subdirectory (libdariadb)

if(ENABLE_SERVER)
  add_subdirectory (network)
endif()

if (ENABLE_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(ENABLE_BENCHMARKS)
  add_subdirectory (benchmarks)
endif()
