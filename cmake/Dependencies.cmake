set(TPL_LINK_LIBS "")

# LLVM 7.0+
find_package(LLVM 7 REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "7")
    message(FATAL_ERROR "LLVM 7 or newer is required.")
endif ()
llvm_map_components_to_libnames(LLVM_LIBRARIES core mcjit nativecodegen native)
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
list(APPEND TPL_LINK_LIBS ${LLVM_LIBRARIES})

# Check Clang++ is available. Ideally, you should have Clang 7 or Clang 6 ...
set(SUPPORTED_CLANGS "clang++-7" "clang++-6.0" "clang++-5.0")
find_program(CLANG NAMES ${SUPPORTED_CLANGS})
if (NOT EXISTS ${CLANG})
    message(FATAL_ERROR "Unable to locate clang++.")
else()
    message(STATUS "Found Clang ${CLANG}")
endif ()