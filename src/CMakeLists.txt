# Setup includes
include_directories("${PROJECT_SOURCE_DIR}/src/include/" "${PROJECT_BINARY_DIR}")

# Setup sources: add all CPP files in 'src', but remove tpl.cpp (since it
# contains main(), and all cross-compiled code.
file(GLOB_RECURSE tpl_sources "${PROJECT_SOURCE_DIR}/src/*.cpp")
list(REMOVE_ITEM tpl_sources
     "${PROJECT_SOURCE_DIR}/src/tpl.cpp"
     "${PROJECT_SOURCE_DIR}/src/vm/gen_opt_bc.cpp"
     "${PROJECT_SOURCE_DIR}/src/vm/bytecode_handlers_ir.cpp")

##################################################
# Generate Bytecode Handlers Logic and Cleaner
#
# TPL cross-compiles bytecode handler logic into LLVM bitcode in order to load
# it at runtime to support JIT. We use the COMPILE_TO_IR() function below to
# cross-compile all necessary code.
#
# We also have a "cleaning" binary called "gen_bc" that accepts bitcode files
# and cleans them to make jitting much faster. Each cross-compiled bitcode file
# becomes a dependency for the "gen_bc" module.
#
##################################################

# Setup compiler arguments to Clang
set(CLANG_IR_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-emit-llvm" "-c")
separate_arguments(CLANG_IR_CXX_FLAGS)
set(CLANG_IR_CXX_INCLUDES "-I${PROJECT_SOURCE_DIR}/src/include" "-isystem${LLVM_INCLUDE_DIRS}")
set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-isystem${PROJECT_SOURCE_DIR}/third_party/gtest/include")

# Function to generate rule to cross compile a source file to an IR module.
# When called called with a 'src.cpp' source file it will generate a 'src_bc'
# target that can be built.
# e.g. COMPILE_TO_IR(test.cpp) generates the "test_bc" make target.
# Modified from Apache Impala.
function(tpl_compile_to_ir SRC_FILE)
    set(FULL_NAME "${PROJECT_SOURCE_DIR}/src/${SRC_FILE}")
    get_filename_component(BASE_NAME ${FULL_NAME} NAME_WE)
    set(OUTPUT_FILE "${CMAKE_BINARY_DIR}/${BASE_NAME}.bc")
    add_custom_command(
        OUTPUT ${OUTPUT_FILE}
        COMMAND ${CLANG} ${CLANG_IR_CXX_FLAGS} ${CLANG_IR_CXX_INCLUDES} ${FULL_NAME} -o ${OUTPUT_FILE}
        DEPENDS ${SRC_FILE})
    add_custom_target(${BASE_NAME}_bc ALL DEPENDS ${OUTPUT_FILE})
    add_dependencies(gen_opt_bc ${BASE_NAME}_bc)
endfunction(tpl_compile_to_ir)

# Build the "gen_opt_bc" executable. This is dependent on all cross-compiled bitcode
# set up through tpl_compile_to_ir()
add_executable(gen_opt_bc "${PROJECT_SOURCE_DIR}/src/vm/gen_opt_bc.cpp")
set_target_properties(gen_opt_bc PROPERTIES ENABLE_EXPORTS true)
target_link_libraries(gen_opt_bc ${TPL_LINK_LIBS})

# Cross-compile the bytecode handlers
tpl_compile_to_ir(vm/bytecode_handlers_ir.cpp)

##################################################
#
# The TPL shared library
#
# We build a shared library containing all the TPL logic so it can be shared
# between the main TPL binary and the unit tests.
#
##################################################

add_library(tpl_shared STATIC ${tpl_sources})

##################################################
#
# TPL executable
#
##################################################

set(tpl_main "${PROJECT_SOURCE_DIR}/src/tpl.cpp")
add_executable(tpl ${tpl_main})
add_dependencies(tpl gen_opt_bc)
set_target_properties(tpl PROPERTIES ENABLE_EXPORTS true)
target_link_libraries(tpl tpl_shared ${TPL_LINK_LIBS})

##################################################
#
# After TPL is built, we need to clean the bitcode module that was
# cross-compiled. Add a post-build target here ...
#
##################################################

add_custom_command(
        TARGET tpl
        POST_BUILD
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating Optimized Bitcode ..."
        COMMAND gen_opt_bc bytecode_handlers_ir.bc bytecode_handlers_opt.bc
        COMMAND mv bytecode_handlers_opt.bc bytecode_handlers_ir.bc)

##################################################
#
# Set the output folders for all binaries and libraries
#
##################################################

set_target_properties(tpl gen_opt_bc tpl_shared
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)