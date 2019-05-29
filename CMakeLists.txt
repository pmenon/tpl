cmake_minimum_required(VERSION 3.4)

project(tpl)

# The version number.
set(TPL_VERSION_MAJOR 0)
set(TPL_VERSION_MINOR 1)

# For external projects
include(ExternalProject)

# Let CMake know where all the modules are (i.e., in ./cmake)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

# Checks from terrier require these
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
find_package(ClangTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif ()

# configure a header file to pass some of the CMake settings to the source code
configure_file (
  "${PROJECT_SOURCE_DIR}/tpl.h.in"
  "${PROJECT_BINARY_DIR}/tpl.h"
)

# Options
option(TPL_USE_ASAN "Build TPL with AddressSanitizer enabled" OFF)
option(TPL_USE_TSAN "Build TPL with ThreadSanitizer enabled" OFF)
option(TPL_USE_GOLD "Use the GNU Gold linker if available" OFF)
option(TPL_GENERATE_COVERAGE "Build TPL with code coverage enabled" OFF)

# Is this MacOS ?
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(MACOSX TRUE)
endif()

############################################################
#
# Compiler Options
#
############################################################

# Default to 'Release' if no build type is provided, but warn the user
if (NOT CMAKE_BUILD_TYPE)
  message(WARNING "No build type selected, default to Release")
  set(CMAKE_BUILD_TYPE "Release")
endif()

# Ensure dependencies
include(Dependencies)

# Setup the compiler and setup the compiler flags
include(SetupCompiler)

############################################################
#
# Project Setup
#
############################################################

# Let the sources and tests directory set themselves up
add_subdirectory(src)
add_subdirectory(test)

############################################################
#
# "make gen-cov" target
#
############################################################

add_custom_target(gen-cov
        DEPENDS check
        COMMAND lcov --directory . --capture --output-file coverage.info &&               # capture coverage info
                lcov --remove coverage.info '/usr/*' --output-file coverage.info          # filter out system
                     --remove coverage.info '*/build/*' --output-file coverage.info       # filter out build
                     --remove coverage.info '*/third_party/*' --output-file coverage.info # filter out third_party
                     --remove coverage.info '*/test/*' --output-file coverage.info        # filter out test
                     --remove coverage.info '*/src/main/*' --output-file coverage.info && # filter out main
                genhtml coverage.info --output-directory coverage-html
        )

############################################################
#
# Checks from Terrier
#
############################################################

###########################################################
#
# "make check-lint" and "make check-censored" targets
#
###########################################################

if (UNIX)

  file(GLOB_RECURSE LINT_FILES
          "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
          "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
          "${CMAKE_CURRENT_SOURCE_DIR}/test/*.h"
          "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp"
          )

  find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
  message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

  # Full lint
  # Balancing act: cpplint.py takes a non-trivial time to launch,
  # so process 12 files per invocation, while still ensuring parallelism
  add_custom_target(check-lint echo '${LINT_FILES}' | xargs -n12 -P8
          ${CPPLINT_BIN}
          --verbose=2 ${TERRIER_LINT_QUIET}
          --linelength=120
          --filter=-legal/copyright,-build/header_guard
          )

  set(CENSOR_FILES ${LINT_FILES})

  # If whitelisting becomes necessary, do it here. Make it harder so that people try not to do it.
  #  list(REMOVE_ITEM CENSOR_FILES
  #
  #      )

  add_custom_target(check-censored
          grep --invert-match -n -e '^ *//' -e '^ *[*]' ${CENSOR_FILES} # check all uncommented lines w/ line num
          | grep -i -f ${BUILD_SUPPORT_DIR}/bad_words.txt               # for bad words, not case sensitive
          | grep --invert-match -e 'NOLINT'                             # the count of them that aren't NOLINT
          || exit 0                                                     # if nothing found, return 0
          && exit 1                                                     # else return 1, note || && left-associative
          )
endif (UNIX)

###########################################################
#
# "make format" and "make check-format" targets
#
###########################################################

# we modified the format script to take multiple args

string(CONCAT FORMAT_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test,"
        )

# runs clang format and updates files in place.
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        --source_dirs ${FORMAT_DIRS}
        --fix ${TERRIER_LINT_QUIET}
        )

# runs clang format and exits with a non-zero exit code if any files need to be reformatted
add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        --source_dirs ${FORMAT_DIRS}
        ${TERRIER_LINT_QUIET}
        )

###########################################################
#
# "make check-clang-tidy" target
#
###########################################################

if (${CLANG_TIDY_FOUND})
  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  # note that clang-tidy automatically looks for a .clang-tidy file in parent directories
  add_custom_target(check-clang-tidy
          ${BUILD_SUPPORT_DIR}/run-clang-tidy.py                            # run LLVM's clang-tidy script
          -clang-tidy-binary ${CLANG_TIDY_BIN}                              # using our clang-tidy binary
          -p ${CMAKE_BINARY_DIR}                                            # using cmake's generated compile commands
          )
  # TODO(WAN): if we're bringing in gbenchmark and gtest properly, we can set them as dependencies here
endif ()

###########################################################
#
# "make check-tpl" and "make check-tpl-diff" targets
#
###########################################################

# runs the TPL binary on all files in 'sample_tpl' comparing each scripts output
# to the reference output in 'tpl_tests.txt'
add_custom_target(check-tpl
        DEPENDS tpl
        COMMAND ${BUILD_SUPPORT_DIR}/run_tpl_tests.py
                -b ${CMAKE_BINARY_DIR}/bin/tpl
                -f ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt
                -t ${PROJECT_SOURCE_DIR}/sample_tpl
        )

# Like 'check-tpl', but runs the test only on added tests to 'tpl_tests.txt'
add_custom_target(check-tpl-diff
        DEPENDS tpl
        COMMAND git diff -U0 ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt
                | grep '^[+][^+-]'
                | sed 's/^.//'
                | uniq > ${CMAKE_BINARY_DIR}/tpl_tests_diff.txt             # create tpl_tests_diff.txt containing the new tests
                &&
                ${BUILD_SUPPORT_DIR}/run_tpl_tests.py                       # run the check only on the new tests
                -b ${CMAKE_BINARY_DIR}/bin/tpl
                -f ${CMAKE_BINARY_DIR}/tpl_tests_diff.txt
                -t ${PROJECT_SOURCE_DIR}/sample_tpl
        )

############################################################
#
# Done, print final configuration summary
#
############################################################

message(STATUS "")
message(STATUS "******************* TPL Configuration Summary *******************")
message(STATUS "")
message(STATUS "General:")
message(STATUS "  Version           :   ${TPL_VERSION_MAJOR}.${TPL_VERSION_MINOR}")
message(STATUS "  System            :   ${CMAKE_SYSTEM_NAME}")
message(STATUS "  Compiler          :   ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION})")
message(STATUS "  Build type        :   ${CMAKE_BUILD_TYPE}")
message(STATUS "  CXX flags         :   ${CMAKE_CXX_FLAGS}")
message(STATUS "  Linker flags      :   ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "")
message(STATUS "*****************************************************************")
message(STATUS "")
