# Setup includes
include_directories("${PROJECT_SOURCE_DIR}/src/include/" "${PROJECT_BINARY_DIR}")

# Include third party libs
include_directories("${PROJECT_SOURCE_DIR}/third_party/spdlog/include")

# Setup sources
file(GLOB_RECURSE tpl_sources "${PROJECT_SOURCE_DIR}/src/*.cpp")
list(REMOVE_ITEM tpl_sources ${PROJECT_SOURCE_DIR}/src/tpl.cpp)

##################################################
# Bytecode Handlers
##################################################

# We need to cross-compile the bytecode handlers into LLVM bitcode so that  we can load
# it in at runtime for JIT. To do this, we make use of the function COMPILE_TO_IR() which
# accepts a source file and generates an LLVM bitcode file. Each bitcode file becomes a
# dependency to the main TPL target.
set(CLANG_IR_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-emit-llvm" "-c")
separate_arguments(CLANG_IR_CXX_FLAGS)
set(CLANG_IR_CXX_INCLUDES
    "-I${PROJECT_SOURCE_DIR}/src/include"
    "-I${PROJECT_SOURCE_DIR}/third_party/spdlog/include"
    "-isystem${LLVM_INCLUDE_DIRS}")

# Function to generate rule to cross compile a source file to an IR module.
# This should be called with the .cc src file and it will generate a
# src-file-ir target that can be built.
# e.g. COMPILE_TO_IR(test.cc) generates the "test-ir" make target.
# Modified from Apache Impala.
function(COMPILE_TO_IR SRC_FILE)
    set(FULL_NAME "${PROJECT_SOURCE_DIR}/src/${SRC_FILE}")
    get_filename_component(BASE_NAME ${FULL_NAME} NAME_WE)
    set(OUTPUT_FILE "${CMAKE_BINARY_DIR}/lib/${BASE_NAME}.bc")
    add_custom_command(
        OUTPUT ${OUTPUT_FILE}
        COMMAND ${CLANG} ${CLANG_IR_CXX_FLAGS} ${CLANG_IR_CXX_INCLUDES} ${FULL_NAME} -o ${OUTPUT_FILE}
        DEPENDS ${SRC_FILE})
    add_custom_target(${BASE_NAME}_bc ALL DEPENDS ${OUTPUT_FILE})
    add_dependencies(tpl bytecode_handlers_bc)
endfunction(COMPILE_TO_IR)

##################################################
# TPL library
##################################################
add_library(tpl_shared STATIC ${tpl_sources})

##################################################
# TPL executable
##################################################
set(tpl_main "${PROJECT_SOURCE_DIR}/src/tpl.cpp")
add_executable(tpl ${tpl_main})
target_link_libraries(tpl tpl_shared ${TPL_LINK_LIBS})

# Set the output folders for all binaries and libraries
set_target_properties(tpl tpl_shared
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Cross-compile the bytecode handlers
COMPILE_TO_IR(vm/bytecode_handlers.cpp)