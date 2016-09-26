cmake_minimum_required (VERSION 3.1)
set(PROJECT "dariadb")
project (${PROJECT} VERSION 0.1.1)

option(ENABLE_TESTING "Enable testing of the dariadb." ON)
option(ENABLE_METRICS "Enable code metrics." ON)
option(ENABLE_INTEGRATION_TESTS "Enable integration test." ON)
option(ENABLE_BOOST_LOGGER "Enable boost log library." ON)
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
MESSAGE(STATUS "ENABLE_TESTING - " ${ENABLE_TESTING})
MESSAGE(STATUS "ENABLE_METRICS - " ${ENABLE_METRICS})
MESSAGE(STATUS "ENABLE_INTEGRATION_TESTS - " ${ENABLE_INTEGRATION_TESTS})
MESSAGE(STATUS "CLANG_MSAN - " ${CLANG_MSAN})
MESSAGE(STATUS "CLANG_ASAN_UBSAN - " ${CLANG_ASAN_UBSAN})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(AddCXXCompilerFlag)

#if(NOT MSVC)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
#endif(NOT MSVC)

set(BOOST_DEP unit_test_framework system filesystem system)

IF(WIN32)
  MESSAGE(STATUS "WIN32:")
  MESSAGE(STATUS "+ boost root: " ${BOOST_ROOT})

  if(MSVC)
    find_package(Boost 1.57.0 COMPONENTS ${BOOST_DEP} REQUIRED)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  /EHsc")
    add_cxx_compiler_flag(-W4)
    #add_cxx_compiler_flag(-Wall)
    add_definitions(-DMSVC)
    add_definitions(-D_ENABLE_ATOMIC_ALIGNMENT_FIX)
  else(MSVC)
    set(BOOST_DEP ${BOOST_DEP} program_options)
    find_package(Boost 1.57.0 REQUIRED ${BOOST_DEP})
  endif(MSVC)

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_WIN32_WINNT=0x0501")
  set(CMAKE_CXX_FLAGS_RELEASE "/Ox /GT /Ot -DNDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Oy") #-fno-omit-frame-pointer analog
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
else(WIN32)
  set(BoostUSESTATIC_LIBS OFF)
  set(BOOST_DEP ${BOOST_DEP} program_options)
  find_package(Boost  REQUIRED ${BOOST_DEP})

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
  #add_cxx_compiler_flag(--coverage COVERAGE)
  #SET(CMAKE_CXX_FLAGS_COVERAGE "${CMAKE_CXX_FLAGS_COVERAGE} --coverage -O0")
ENDIF(WIN32)

IF(Boost_FOUND)
  INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
  LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
else(Boost_FOUND)
  MESSAGE(FATAL_ERROR "boost not found")
  EXIT()
ENDIF(Boost_FOUND)

find_package (Threads)

if(CMAKE_COMPILER_IS_GNUCXX)
  MESSAGE(STATUS "gcc compiller")
  add_definitions(-DGNU_CPP)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pipe")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -ftemplate-backtrace-limit=0")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -pedantic-errors -Wno-pedantic")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wwrite-strings")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wdeprecated-declarations")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftest-coverage")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -fno-inline -g3 -fstack-protector-all -DDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g0 -march=native -mtune=native -DNDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-omit-frame-pointer")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-strict-aliasing -Werror=pragmas") #boost-shared-mutex error;
endif(CMAKE_COMPILER_IS_GNUCXX)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  MESSAGE(STATUS "clang compiller")
  add_definitions(-DCLANG_CPP)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -ftemplate-backtrace-limit=0")
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
  #jsonpp
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-switch-enum -Wno-switch-default -Wno-covered-switch-default")
  #spdlog
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -fno-inline -g3 -fstack-protector-all -DDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g0 -march=native -mtune=native -DNDEBUG")

  if(CLANG_ASAN_UBSAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
  endif(CLANG_ASAN_UBSAN)
  if(CLANG_MSAN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory -fsanitize-memory-track-origins")
  endif(CLANG_MSAN)  
endif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")


if (ENABLE_METRICS)
  add_definitions(-DENABLE_METRICS)
endif(ENABLE_METRICS)

include_directories(extern)
include_directories(extern/spdlog/include)
add_subdirectory (libdariadb)
add_subdirectory (network)
include_directories(network)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

if (ENABLE_TESTING)
   enable_testing()
   add_subdirectory(tests)
endif(ENABLE_TESTING)

add_subdirectory (benchmarks)
