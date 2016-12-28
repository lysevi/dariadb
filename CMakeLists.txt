cmake_minimum_required (VERSION 3.1)
set(PROJECT "dariadb")
project (${PROJECT} VERSION 0.1.1)

IF(NOT CMAKE_BUILD_TYPE)
        message(STATUS "CMAKE_BUILD_TYPE is not set, set to default = RELWITHDEBINFO")
        SET(CMAKE_BUILD_TYPE "RELWITHDEBINFO")
ENDIF()

MESSAGE( STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE} )

option(ENABLE_TESTS "Enable testing of the dariadb." ON)
option(ENABLE_METRICS "Enable code metrics." ON)
option(ENABLE_INTEGRATION_TESTS "Enable integration test." ON)
option(ENABLE_SERVER "Enable build dariadb server." ON)
option(ENABLE_BENCHMARKS "Enable build dariadb benchmarks." ON)
option(CLANG_ASAN_UBSAN "Enable Clang address & undefined behavior sanitizer for binary." OFF)
option(CLANG_MSAN "Enable Clang memory sanitizer for binary." OFF)

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

if(ENABLE_TESTS)
  set(BOOST_DEP ${BOOST_DEP} unit_test_framework)
endif()

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

if (ENABLE_METRICS)
  add_definitions(-DENABLE_METRICS)
endif()

include_directories(extern)
include_directories(extern/spdlog/include)
include_directories(extern/stx-btree/include)

add_subdirectory(extern/libsqlite3)
include_directories(extern/libsqlite3)

set_target_properties(sqlite3 PROPERTIES FOLDER Extern)

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

add_subdirectory (tools)
