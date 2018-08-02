cmake_minimum_required(VERSION 3.4)

project(tpl)

# The version number.
set(TPL_VERSION_MAJOR 0)
set(TPL_VERSION_MINOR 1)

# Enable ExternalProject CMake module
include(ExternalProject)
 
# configure a header file to pass some of the CMake settings to the source code
configure_file (
  "${PROJECT_SOURCE_DIR}/tpl.h.in"
  "${PROJECT_BINARY_DIR}/tpl.h"
  )

option(TPL_USE_ASAN "Build TPL with AddressSanitizer enabled" OFF)
option(TPL_USE_TSAN "Build TPL with ThreadSanitizer enabled" OFF)

function(print_config_summary)
  set(BUILD_TYPE "CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}")
  string(TOUPPER ${BUILD_TYPE} BUILD_TYPE)
  set(CXX_FLAGS "${${BUILD_TYPE}}")

  message(STATUS "")
  message(STATUS "******************* TPL Configuration Summary *******************")
  message(STATUS "General:")
  message(STATUS "  Version           :   ${TPL_VERSION_MAJOR}.${TPL_VERSION_MINOR}")
  message(STATUS "  System            :   ${CMAKE_SYSTEM_NAME}")
  message(STATUS "  Compiler          :   ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION})")
  message(STATUS "  Build type        :   ${CMAKE_BUILD_TYPE}")
  message(STATUS "  CXX flags         :   ${CXX_FLAGS}")
  message(STATUS "")
  message(STATUS "Dependencies:")
  message(STATUS "  Linker flags      :   ${CMAKE_EXE_LINKER_FLAGS}")
  message(STATUS "")
endfunction()

############################################################
#
# Compiler Options
#
############################################################

# ---- Force C++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "-Wall")

if (NOT CMAKE_BUILD_TYPE)
  message(WARNING "No build type selected, default to Release")
  set(CMAKE_BUILD_TYPE "Release")
endif()

# -- [ Debug Flags
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb3 -fno-inline -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO  "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

# ---- Require Clang or GCC
if(NOT (("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang") OR ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")))
  message(SEND_ERROR "TPL only supports Clang or GCC")
endif()

# ---- Use Gold Linker?
set(USE_GOLD true CACHE BOOL "Use the GNU Gold linker if available")
if(USE_GOLD)
  execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=gold -Wl,--version ERROR_QUIET OUTPUT_VARIABLE LD_VERSION)
  if("${LD_VERSION}" MATCHES "GNU gold")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
    message(STATUS "GNU gold found")
  else()
    message(STATUS "GNU gold not found")
    set(USE_GOLD OFF)
  endif()
endif()

# ---- Setup AddressSanitizer and ThreadSanitizer if configured

# Clang/GCC don't allow both ASan and TSan to be enabled together
if("${TPL_USE_ASAN}" AND "${TPL_USE_TSAN}")
  message(SEND_ERROR "Can only enable one of ASAN or TSAN at a time")
endif()

if(${TPL_USE_ASAN})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  message(STATUS "AddressSanitizer enabled.")
elseif(${TPL_USE_TSAN})
  add_definitions("-fsanitize=thread")
  message(STATUS "ThreadSanitizer enabled.")
endif()

############################################################
#
# Project Setup
#
############################################################

# Setup tests
set(tpl_test_shared tpl_shared)
add_subdirectory(src)
add_subdirectory(test)

# Print final configuration summary
print_config_summary()
