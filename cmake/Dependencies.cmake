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

############################################################
# JeMalloc
############################################################

find_package(JeMalloc REQUIRED)
include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
list(APPEND TPL_LINK_LIBS ${JEMALLOC_LIBRARIES})

############################################################
# LLVM
############################################################

# Look for LLVM 11+
find_package(LLVM 11 REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "11")
    message(FATAL_ERROR "LLVM 11 or newer is required.")
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

set(SUPPORTED_CLANGS "clang++-11" "clang++-10" "clang++-9")
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

FetchContent_Declare(ips4o GIT_REPOSITORY https://github.com/SaschaWitt/ips4o)
FetchContent_MakeAvailable(ips4o)
include_directories(SYSTEM "${ips4o_SOURCE_DIR}")
message(STATUS "IPS4O include dir: ${ips4o_SOURCE_DIR}")

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
    FetchContent_Declare(spdlog URL ${SPDLOG_SOURCE_URL})
    FetchContent_MakeAvailable(spdlog)
    include_directories(SYSTEM "${spdlog_SOURCE_DIR}/include")
    #set_property(TARGET spdlog PROPERTY POSITION_INDEPENDENT_CODE ON)
    if (APPLE)
        set_property(TARGET spdlog PROPERTY CXX_VISIBILITY_PRESET hidden)
    endif ()
else ()
    set(SPDLOG_VENDORED 0)
    find_package(spdlog REQUIRED)
endif ()
message(STATUS "Fetched SpdLog ${SPDLOG_VERSION}")
list(APPEND TPL_LINK_LIBS spdlog::spdlog)

############################################################
# XByak
############################################################

if (DEFINED ENV{TPL_XBYAK_URL})
    set(XBYAK_SOURCE_URL "$ENV{TPL_XBYAK_URL}")
else ()
    set(XBYAK_SOURCE_URL "https://github.com/herumi/xbyak/archive/v${XBYAK_VERSION}.tar.gz")
endif ()

FetchContent_Declare(xbyak URL ${XBYAK_SOURCE_URL})
FetchContent_MakeAvailable(xbyak)
include_directories(SYSTEM "${xbyak_SOURCE_DIR}")
message(STATUS "Fetched Xbyak ${XBYAK_VERSION}")

############################################################
# CSV Parser
############################################################

include_directories(SYSTEM "${THIRD_PARTY_DIR}/csv-parser")

############################################################
# Google Test
############################################################

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        release-${GTEST_VERSION}
)
FetchContent_MakeAvailable(googletest)
message(STATUS "Fetched GTest ${GTEST_VERSION}")

############################################################
# Google Benchmark
############################################################

set(BENCHMARK_ENABLE_TESTING OFF CACHE INTERNAL "")      # Disable benchmark tests.
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE INTERNAL "")  # Disable benchmark tests.
FetchContent_Declare(
        gbenchmark
        GIT_REPOSITORY https://github.com/google/benchmark
        GIT_TAG        v${GBENCHMARK_VERSION}
)
FetchContent_MakeAvailable(gbenchmark)
message(STATUS "Fetched Google Benchmark ${GBENCHMARK_VERSION}")
