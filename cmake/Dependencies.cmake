# LLVM 6.0+
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "6.0")
    message(FATAL_ERROR "LLVM 6.0 or newer is required.")
endif ()
llvm_map_components_to_libnames(LLVM_LIBRARIES core mcjit nativecodegen native)
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
list(APPEND TERRIER_LINK_LIBS ${LLVM_LIBRARIES})