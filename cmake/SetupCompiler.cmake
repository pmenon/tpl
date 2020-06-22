# ---- Force C++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---- Require Clang or GCC
if (NOT (("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang") OR
         ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")   OR
         ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")))
    message(SEND_ERROR "TPL only supports Clang or GCC")
endif ()

# ---- Setup initial CXX flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++2a -Wall -Werror -march=native")

############################################################
#
# Configure all CXX flags
#
############################################################

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=FastDebug
#   Same as DEBUG, except with some optimizations on.
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
# For CMAKE_BUILD_TYPE=RelWithDebInfo
#   -O2: Some compiler optimizations
#   -ggdb: Enable gdb debugging
set(CXX_FLAGS_DEBUG "-O0 -ggdb -fno-inline -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CXX_FLAGS_FASTDEBUG "-O1 -ggdb -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CXX_FLAGS_RELEASE "-O3 -DNDEBUG -fomit-frame-pointer -funroll-loops")
set(CXX_FLAGS_RELWITHDEBINFO "-O2 -ggdb -DNDEBUG -fno-omit-frame-pointer")

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_FASTDEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_RELEASE}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELWITHDEBINFO")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_RELWITHDEBINFO}")
else ()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

############################################################
#
# Color-ize output, if enabled.
#
############################################################

if (${TPL_COLORIZE_OUTPUT})
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")
    else ()
        message(WARNING "Unable to colorize output of unknown compiler: ${CMAKE_CXX_COMPILER_ID}")
    endif ()
endif ()

############################################################
#
# LLD linker setup
#
############################################################

if (TPL_USE_LLD)
    execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=lld -Wl,--version ERROR_QUIET OUTPUT_VARIABLE LD_VERSION)
    if ("${LD_VERSION}" MATCHES "LLD")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
        message(STATUS "LLD enabled")
    else ()
        message(STATUS "LLD not found - disabled")
        set(TPL_USE_LLD OFF)
    endif ()
endif ()

############################################################
# All executables that rely on code-generation need to be
# correcty linked with -rdynamic; otherwise, LLVM can't find
# dependent TPL symbols at runtime.
#
# Rather than setting ENABLE_EXPORTS globally (as below),
# each target has to explicitly enable the ENABLE_EXPORTS
# target property.
############################################################

#set(CMAKE_ENABLE_EXPORTS true)

############################################################
#
# AddressSanitizer / ThreadSanitizer setup
#
############################################################

# Clang and GCC don't allow both ASan and TSan to be enabled together
if ("${TPL_USE_ASAN}" AND "${TPL_USE_TSAN}")
    message(SEND_ERROR "Can only enable one of ASAN or TSAN at a time")
endif ()

if (${TPL_USE_ASAN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
    message(STATUS "AddressSanitizer (ASAN) enabled")
elseif (${TPL_USE_TSAN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    message(STATUS "ThreadSanitizer (TSAN) enabled")
endif ()

############################################################
#
# JeMalloc flags
#
############################################################

if (${JEMALLOC_FOUND})
    set(JEMALLOC_LINK_FLAGS "-Wl,--no-as-needed")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${JEMALLOC_LINK_FLAGS}")
endif ()

############################################################
#
# Code Coverage
#
############################################################

if (TPL_GENERATE_COVERAGE)
    if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
        message(FATAL_ERROR "Coverage can only be generated with a debug build type!")
    endif ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
endif ()