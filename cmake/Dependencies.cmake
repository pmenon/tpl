set(TPL_LINK_LIBS "")

set(THIRD_PARTY_DIR "${PROJECT_SOURCE_DIR}/third_party")

############################################################
# JeMalloc
############################################################

find_package(JeMalloc REQUIRED)
include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
list(APPEND TPL_LINK_LIBS ${JEMALLOC_LIBRARIES})

############################################################
# LLVM
############################################################

# Look for LLVM 7+
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "7")
    message(FATAL_ERROR "LLVM 7 or newer is required.")
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

option(BUILD_XXHSUM "Disable building xxhsum binary" OFF)
option(XXHASH_BUNDLED_MODE "Indicate that we're building in bundled mode" ON)
add_subdirectory(${THIRD_PARTY_DIR}/xxHash/cmake_unofficial)
include_directories(SYSTEM "${THIRD_PARTY_DIR}/xxHash")

############################################################
# Libcount
############################################################

add_subdirectory(${THIRD_PARTY_DIR}/libcount)
include_directories(SYSTEM "${THIRD_PARTY_DIR}/libcount/include")
list(APPEND TPL_LINK_LIBS count)

############################################################
# Check Clang++ is available. Ideally, you should have
# either Clang 7 or 6, otherwise your compiler is too
# old to use with TPL.
############################################################

set(SUPPORTED_CLANGS "clang++-7" "clang++-6.0")
if (${MACOSX})
    # Because MacOS does some weird Clang versioning, and it isn't available
    # through Homebrew, we add in vanilla "clang++". You won't be running TPL
    # in production on a Mac system anyways ...
    list(APPEND SUPPORTED_CLANGS "clang++")
endif ()

find_program(CLANG NAMES ${SUPPORTED_CLANGS})
if (NOT EXISTS ${CLANG})
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

include_directories(SYSTEM "${THIRD_PARTY_DIR}/spdlog/include")

############################################################
# XByak
############################################################

include_directories(SYSTEM "${THIRD_PARTY_DIR}/xbyak")

############################################################
# Google Test
############################################################

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third_party/gtest/include)
