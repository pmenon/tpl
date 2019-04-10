set(TPL_LINK_LIBS "")

############################################################
# LLVM 7.0+
############################################################

find_package(LLVM 7 REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "7")
    message(FATAL_ERROR "LLVM 7 or newer is required.")
endif ()
llvm_map_components_to_libnames(LLVM_LIBRARIES core mcjit nativecodegen native ipo)
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
list(APPEND TPL_LINK_LIBS ${LLVM_LIBRARIES})

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

include_directories(SYSTEM "${PROJECT_SOURCE_DIR}/third_party/ips4o")

############################################################
# SPD Log - The logging library
############################################################

include_directories(SYSTEM "${PROJECT_SOURCE_DIR}/third_party/spdlog/include")

############################################################
# XByak
############################################################

include_directories(SYSTEM "${PROJECT_SOURCE_DIR}/third_party/xbyak")

############################################################
# JSON
############################################################

include_directories(SYSTEM "${PROJECT_SOURCE_DIR}/third_party/nlohmann")


# TBB
# Intel TBB
find_package(TBB REQUIRED)
include_directories(SYSTEM ${TBB_INCLUDE_DIRS})
list(APPEND TPL_LINK_LIBS ${TBB_LIBRARIES})
