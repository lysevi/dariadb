cmake_minimum_required (VERSION 2.8)
project (dariadb)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(AddCXXCompilerFlag)

set(dariadb_MAJOR_VERSION 0)
set(dariadb_MINOR_VERSION 0)
set(dariadb_PATCH_VERSION 0)
set(dariadb_VERSION${dariadb_MAJOR_VERSION}.${dariadb_MINOR_VERSION}.${dariadb_PATCH_VERSION})

# Offer the user the choice of overriding the installation directories
set(INSTALL_LIB_DIR lib CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR bin CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR include CACHE PATH "Installation directory for header files")

if(WIN32 AND NOT CYGWIN)
  set(DEF_INSTALL_CMAKE_DIR CMake)
else()
  set(DEF_INSTALL_CMAKE_DIR lib/CMake/dariadb)
endif()
set(INSTALL_CMAKE_DIR ${DEF_INSTALL_CMAKE_DIR} CACHE PATH "Installation directory for CMake files")
  
# Make relative paths absolute (needed later on)
foreach(p LIB BIN INCLUDE CMAKE)
  set(var INSTALL_${p}_DIR)
  if(NOT IS_ABSOLUTE "${${var}}")
    set(${var} "${CMAKE_INSTALL_PREFIX}/${${var}}")
  endif()
endforeach()

set(Boost_USE_STATIC_LIBS        OFF)
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME     OFF)
set(BOOST_ALL_DYN_LINK           ON)

if(NOT MSVC)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endif(NOT MSVC)

IF(WIN32)
  MESSAGE(STATUS "WIN32:")
  MESSAGE(STATUS "+ boost root: " ${BOOST_ROOT})

  if(MSVC)
    find_package(Boost 1.57.0 REQUIRED unit_test_framework system  filesystem)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  /EHsc")
    add_cxx_compiler_flag(-W4)
    add_definitions(-DMSVC)
  else(MSVC)
    find_package(Boost 1.57.0 REQUIRED unit_test_framework system  filesystem)
  endif(MSVC)

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_WIN32_WINNT=0x0501")
  set(CMAKE_CXX_FLAGS_RELEASE "/Ox /GT /Ot -DNDEBUG")
else(WIN32)
  set(BoostUSESTATIC_LIBS OFF)
  
  find_package(Boost  REQUIRED unit_test_framework system  filesystem)

  set(CMAKE_CXX_FLAGS_COVERAGE "${CMAKE_CXX_FLAGS_DEBUG}" CACHE STRING
    "Flags used by the C++ compiler during coverage builds."
    FORCE)
  set(CMAKE_EXE_LINKER_FLAGS_COVERAGE
    "${CMAKE_EXE_LINKER_FLAGS_DEBUG}" CACHE STRING
    "Flags used for linking binaries during coverage builds."
    FORCE)
  set(CMAKE_SHARED_LINKER_FLAGS_COVERAGE
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG}" CACHE STRING
    "Flags used by the shared libraries linker during coverage builds."
    FORCE)
  mark_as_advanced(
    CMAKE_CXX_FLAGS_COVERAGE
    CMAKE_EXE_LINKER_FLAGS_COVERAGE
    CMAKE_SHARED_LINKER_FLAGS_COVERAGE)
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
    "Choose the type of build, options are: None Debug Release Coverage."
    FORCE)
  add_cxx_compiler_flag(--coverage COVERAGE)
ENDIF(WIN32)

IF(Boost_FOUND)
  INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
  LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
else(Boost_FOUND)
  MESSAGE(FATAL_ERROR "boost not found")
  EXIT()
ENDIF(Boost_FOUND)

if(CMAKE_COMPILER_IS_GNUCXX)
  MESSAGE(STATUS "gcc compiller")
  add_definitions(-DGNU_CPP)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -ftemplate-backtrace-limit=0")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -Werror -pedantic-errors")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wwrite-strings")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wdeprecated-declarations")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftest-coverage")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_LOG_DYN_LINK=1")
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -fno-inline -g3 -fstack-protector-all")
  set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g0 -march=native -mtune=native -DNDEBUG")
  #set(CMAKE_CXX_FLAGS_DEBUG  "${CMAKE_CXX_FLAGS_DEBUG} -pg")
  #set(CMAKE_MODULE_LINKER_FLAGS "-pg")
  #set(CMAKE_SHARED_LINKER_FLAGS "-pg")
endif(CMAKE_COMPILER_IS_GNUCXX)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  MESSAGE(STATUS "clang compiller")
  add_definitions(-DCLANG_CPP)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -ftemplate-backtrace-limit=0")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Weverything -Werror -pedantic-errors")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-declarations -Wno-error=deprecated")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-c++98-compat -Wno-c++98-compat-pedantic")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-return-stack-address -Wno-undef")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable -Wno-unused-parameter")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-weak-vtables -Wno-padded")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-global-constructors -Wno-exit-time-destructors")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-shorten-64-to-32 -Wno-sign-conversion")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-prototypes -Wno-missing-variable-declarations")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-shadow -Wno-old-style-cast")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-documentation -Wno-documentation-unknown-command")
  #main
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-cast-align -Wno-disabled-macro-expansion")
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -fno-inline -g3 -fstack-protector-all")
  set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g0 -march=native -mtune=native -DNDEBUG")
endif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")

option(ENABLE_TESTING "Enable testing of the dariadb." ON)
option(USE_MUTEX "use mutex or Locker (OFF value)" ON)

if(USE_MUTEX)
   MESSAGE(STATUS "def USE_MUTEX")
   add_definitions(-DUSE_MUTEX)
else(USE_MUTEX)
   MESSAGE(STATUS "undef USE_MUTEX")
endif(USE_MUTEX)

add_subdirectory (libdariadb)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

if (ENABLE_TESTING)
   enable_testing()
   add_subdirectory(tests)
endif(ENABLE_TESTING)


add_subdirectory (benchmarks)
