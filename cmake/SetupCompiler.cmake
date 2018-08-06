# ---- Force C++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---- Require Clang or GCC
if(NOT (("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang") OR
        ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")   OR
        ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")))
    message(SEND_ERROR "TPL only supports Clang or GCC")
endif()

# ---- Use Gold Linker?
if(USE_GOLD)
    execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=gold -Wl,--version ERROR_QUIET OUTPUT_VARIABLE LD_VERSION)
    if("${LD_VERSION}" MATCHES "GNU gold")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold -Wl,--disable-new-dtags")
        message(STATUS "GNU gold found")
    else()
        message(STATUS "GNU gold not found")
        set(USE_GOLD OFF)
    endif()
endif()

# Clang/GCC don't allow both ASan and TSan to be enabled together
if("${TPL_USE_ASAN}" AND "${TPL_USE_TSAN}")
    message(SEND_ERROR "Can only enable one of ASAN or TSAN at a time")
endif()

if(${TPL_USE_ASAN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    message(STATUS "AddressSanitizer enabled.")
elseif(${TPL_USE_TSAN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    message(STATUS "ThreadSanitizer enabled.")
endif()