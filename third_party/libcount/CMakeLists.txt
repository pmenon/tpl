cmake_minimum_required (VERSION 3.10)

project(libcount)

if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    message(FATAL_ERROR "DO NOT BUILD in-tree.")
endif()

option(LIBCOUNT_BUILD_EXAMPLES "Build examples" OFF)
option(LIBCOUNT_BUILD_CERTIFY "Build libcount certification program" OFF)
option(LIBCOUNT_BUILD_GENERATOR "Build data generator" OFF)

# Generic CXX flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -Werror -march=native")

# Default to 'Release' if no build type is provided, but warn the user
if (NOT CMAKE_BUILD_TYPE)
    message(WARNING "No build type selected, default to Release")
    set(CMAKE_BUILD_TYPE "Release")
endif()

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Optimization CXX flags
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb -O0 -fno-omit-frame-pointer -fno-optimize-sibling-calls")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELWITHDEBINFO")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb -O2 -DNDEBUG -fno-omit-frame-pointer")
else ()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Add libraries
add_subdirectory(count)

# Built examples if instructed
if (${LIBCOUNT_BUILD_EXAMPLES} OR ${LIBCOUNT_BUILD_CERTIFY})
    add_subdirectory(examples)
endif()

# Build data generator if instructed
if (${LIBCOUNT_BUILD_GENERATOR})
    add_subdirectory(util)
endif()
