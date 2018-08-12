cmake_minimum_required(VERSION 3.4)

project(tpl)

# The version number.
set(TPL_VERSION_MAJOR 0)
set(TPL_VERSION_MINOR 1)

# Let CMake know where all the modules are (i.e., in ./cmake)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
 
# configure a header file to pass some of the CMake settings to the source code
configure_file (
  "${PROJECT_SOURCE_DIR}/tpl.h.in"
  "${PROJECT_BINARY_DIR}/tpl.h"
)

# Options
option(TPL_USE_ASAN "Build TPL with AddressSanitizer enabled" OFF)
option(TPL_USE_TSAN "Build TPL with ThreadSanitizer enabled" OFF)
option(TPL_USE_GOLD "Use the GNU Gold linker if available" ON)

############################################################
#
# Compiler Options
#
############################################################

# Default to 'Release' if no build type is provided, but warn the user
if (NOT CMAKE_BUILD_TYPE)
  message(WARNING "No build type selected, default to Release")
  set(CMAKE_BUILD_TYPE "Release")
endif()

# Setup the compiler and setup the compiler flags
include(SetupCompiler)

############################################################
#
# Project Setup
#
############################################################

# Let the sources and tests directory set themselves up
set(tpl_test_shared tpl_shared)
add_subdirectory(src)
add_subdirectory(test)

############################################################
#
# Done, print final configuration summary
#
############################################################

message(STATUS "")
message(STATUS "******************* TPL Configuration Summary *******************")
message(STATUS "")
message(STATUS "General:")
message(STATUS "  Version           :   ${TPL_VERSION_MAJOR}.${TPL_VERSION_MINOR}")
message(STATUS "  System            :   ${CMAKE_SYSTEM_NAME}")
message(STATUS "  Compiler          :   ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION})")
message(STATUS "  Build type        :   ${CMAKE_BUILD_TYPE}")
message(STATUS "  CXX flags         :   ${CMAKE_CXX_FLAGS}")
message(STATUS "  Linker flags      :   ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "")
message(STATUS "*****************************************************************")
message(STATUS "")
