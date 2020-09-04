set(TPL_LINK_LIBS "")

set(THIRD_PARTY_DIR "${PROJECT_SOURCE_DIR}/third_party")

############################################################
# Versions and URLs for toolchain builds, which also can be
# used to configure offline builds
############################################################

# Read toolchain versions from thirdparty/versions.txt
file(STRINGS "${THIRD_PARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach (_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
    # Exclude comments
    if (_VERSION_ENTRY MATCHES "#.*")
        continue()
    endif ()

    string(REGEX MATCH "^[^=]*" _LIB_NAME ${_VERSION_ENTRY})
    string(REPLACE "${_LIB_NAME}=" "" _LIB_VERSION ${_VERSION_ENTRY})

    # Skip blank or malformed lines
    if (${_LIB_VERSION} STREQUAL "")
        continue()
    endif ()

    set(${_LIB_NAME} "${_LIB_VERSION}")
endforeach ()

if (DEFINED ENV{TERRIER_GTEST_URL})
    set(GTEST_SOURCE_URL "$ENV{TERRIER_GTEST_URL}")
else ()
    set(GTEST_SOURCE_URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
endif ()

if (DEFINED ENV{TERRIER_GBENCHMARK_URL})
    set(GBENCHMARK_SOURCE_URL "$ENV{TERRIER_GBENCHMARK_URL}")
else ()
    set(GBENCHMARK_SOURCE_URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz")
endif ()

############################################################
# All external projection use EP_CXX_FLAGS
############################################################

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if (NOT TPL_VERBOSE_THIRDPARTY_BUILD)
set(EP_LOG_OPTIONS
        LOG_CONFIGURE 1
        LOG_BUILD 1
        LOG_INSTALL 1
        LOG_DOWNLOAD 1)
else ()
    set(EP_LOG_OPTIONS)
endif ()


############################################################
# JeMalloc
############################################################

find_package(JeMalloc REQUIRED)
include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
list(APPEND TPL_LINK_LIBS ${JEMALLOC_LIBRARIES})

############################################################
# LLVM
############################################################

# Look for LLVM 10+
find_package(LLVM 10.0 REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "10")
    message(FATAL_ERROR "LLVM 10 or newer is required.")
endif ()
llvm_map_components_to_libnames(LLVM_LIBRARIES core mcjit nativecodegen native ipo)
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
list(APPEND TPL_LINK_LIBS ${LLVM_LIBRARIES})

############################################################
# Intel TBB
############################################################

find_package(TBB REQUIRED)
include_directories(SYSTEM ${TBB_INCLUDE_DIRS})
list(APPEND TPL_LINK_LIBS ${TBB_LIBRARIES})

############################################################
# xxHash
############################################################

set(BUILD_XXHSUM OFF CACHE BOOL "Suppress building xxhsum binary" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Suppress building library since we use headers only" FORCE)
set(XXHASH_BUNDLED_MODE ON CACHE BOOL "Indicate that we're building in bundled mode" FORCE)
add_subdirectory(${THIRD_PARTY_DIR}/xxHash/cmake_unofficial)
include_directories(SYSTEM "${THIRD_PARTY_DIR}/xxHash")

############################################################
# Libcount
############################################################

add_subdirectory(${THIRD_PARTY_DIR}/libcount)
include_directories(SYSTEM "${THIRD_PARTY_DIR}/libcount/include")
list(APPEND TPL_LINK_LIBS libcount)

############################################################
# Check Clang++ is available. Ideally, you should have
# either Clang 7 or 6, otherwise your compiler is too
# old to use with TPL.
############################################################

set(SUPPORTED_CLANGS "clang++-10" "clang++-9" "clang++-8" "clang++-7")
if (${MACOSX})
    # Because MacOS does some weird Clang versioning, and it isn't available
    # through Homebrew, we add in vanilla "clang++". You won't be running TPL
    # in production on a Mac system anyways ...
    list(APPEND SUPPORTED_CLANGS "clang++")
endif ()

find_program(CLANG NAMES ${SUPPORTED_CLANGS})
if (${CLANG} STREQUAL "CLANG-NOTFOUND")
    message(FATAL_ERROR "Unable to locate clang++.")
else()
    message(STATUS "Found Clang ${CLANG}")
endif ()


############################################################
# IPS4O - The sorting library
############################################################

include_directories(SYSTEM "${THIRD_PARTY_DIR}/ips4o")

############################################################
# SPD Log - The logging library
############################################################

if (DEFINED ENV{TPL_SPDLOG_URL})
    set(SPDLOG_SOURCE_URL "$ENV{TPL_SPDLOG_URL}")
else ()
    set(SPDLOG_SOURCE_URL "https://github.com/gabime/spdlog/archive/v${SPDLOG_VERSION}.tar.gz")
endif ()

add_definitions(-DSPDLOG_COMPILED_LIB)
if ("${SPDLOG_HOME}" STREQUAL "")
    FetchContent_Declare(SPDLOG URL ${SPDLOG_SOURCE_URL})
    FetchContent_MakeAvailable(SPDLOG)
    include_directories(SYSTEM "${spdlog_SOURCE_DIR}/include")
    #set_property(TARGET spdlog PROPERTY POSITION_INDEPENDENT_CODE ON)
    if (APPLE)
        set_property(TARGET spdlog PROPERTY CXX_VISIBILITY_PRESET hidden)
    endif ()
else ()
    set(SPDLOG_VENDORED 0)
    find_package(spdlog REQUIRED)
endif ()
list(APPEND TPL_LINK_LIBS spdlog::spdlog)

############################################################
# XByak
############################################################

include_directories(SYSTEM "${THIRD_PARTY_DIR}/xbyak")

############################################################
# CSV Parser
############################################################

include_directories(SYSTEM "${THIRD_PARTY_DIR}/csv-parser")

############################################################
# Google Test
############################################################

if (APPLE)
    set(GTEST_CMAKE_CXX_FLAGS "-DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes -fvisibility=hidden")
endif ()

set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} ${GTEST_CMAKE_CXX_FLAGS}")

if(UPPERCASE_BUILD_TYPE MATCHES DEBUG)
    set(GTEST_LIBRARY_EXTENSION "d${CMAKE_STATIC_LIBRARY_SUFFIX}")
else()
    set(GTEST_LIBRARY_EXTENSION "${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif()

set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
set(GTEST_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${GTEST_LIBRARY_EXTENSION}")
set(GTEST_MAIN_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${GTEST_LIBRARY_EXTENSION}")

# Flags to pass to Googletest
set(GTEST_CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}
        -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})

# Add as an external project
ExternalProject_Add(googletest_ep
        EXCLUDE_FROM_ALL ON
        URL ${GTEST_SOURCE_URL}
        BUILD_BYPRODUCTS ${GTEST_STATIC_LIB} ${GTEST_MAIN_STATIC_LIB}
        CMAKE_ARGS ${GTEST_CMAKE_ARGS}
        ${EP_LOG_OPTIONS})

message(STATUS "GTest version: ${GTEST_VERSION}")
message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
message(STATUS "GTest Main static library: ${GTEST_MAIN_STATIC_LIB}")

include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
add_library(gtest STATIC IMPORTED)
add_library(gtest_main STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION "${GTEST_STATIC_LIB}")
set_target_properties(gtest_main PROPERTIES IMPORTED_LOCATION "${GTEST_MAIN_STATIC_LIB}")
add_dependencies(gtest googletest_ep)
add_dependencies(gtest_main googletest_ep)

############################################################
# Google Benchmark
############################################################

if (TPL_BUILD_BENCHMARKS)
    # Setup the flags for Google Benchmark
    set(GBENCHMARK_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")

    # Prefix directory
    set(GBENCHMARK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
    set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
    set(GBENCHMARK_STATIC_LIB "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}")

    # Set-up flags manually
    set(GBENCHMARK_CMAKE_ARGS
            -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_INSTALL_PREFIX:PATH=${GBENCHMARK_PREFIX}
            -DBENCHMARK_ENABLE_TESTING=OFF
            -DCMAKE_CXX_FLAGS=${GBENCHMARK_CMAKE_CXX_FLAGS})
    if (APPLE)
        set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DBENCHMARK_USE_LIBCXX=ON")
    endif ()

    ExternalProject_Add(gbenchmark_ep
            EXCLUDE_FROM_ALL ON
            URL ${GBENCHMARK_SOURCE_URL}
            BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
            CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS}
            ${EP_LOG_OPTIONS})

    message(STATUS "Google Benchmark version: ${GBENCHMARK_VERSION}")
    message(STATUS "Google Benchmark include dir: ${GBENCHMARK_INCLUDE_DIR}")
    message(STATUS "Google Benchmark static library: ${GBENCHMARK_STATIC_LIB}")
    include_directories(SYSTEM ${GBENCHMARK_INCLUDE_DIR})

    add_library(benchmark STATIC IMPORTED)
    set_target_properties(benchmark PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_STATIC_LIB}")
    add_dependencies(benchmark gbenchmark_ep)
endif ()
