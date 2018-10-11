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
