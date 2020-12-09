enable_testing()

###############################################
# Find all test sources. Then, setup 'make check'
# target. We'll add each test as a dependency to
# the target below.
###############################################

file(GLOB_RECURSE tpl_test_srs "${PROJECT_SOURCE_DIR}/test/*.cpp")

# LSAN_OPTIONS. Required to suppress TBB leaks.
set(TPL_LSAN_OPTIONS "fast_unwind_on_malloc=0,suppressions=${BUILD_SUPPORT_DIR}/leaksanitizer.conf")

add_custom_target(check
        COMMAND LSAN_OPTIONS=${TPL_LSAN_OPTIONS} ${CMAKE_CTEST_COMMAND} ${CTEST_FLAGS} --output-on-failure)

# From list of test files we'll create tests test_name.cpp -> test_name
foreach(test_src ${tpl_test_srs})
    # Get the name of the test from the name
    get_filename_component(test_name ${test_src} NAME_WE)

    # Build a binary for it
    add_executable(${test_name} EXCLUDE_FROM_ALL ${test_src})
    target_include_directories(${test_name} PRIVATE "${PROJECT_SOURCE_DIR}/test/include")
    target_link_libraries(${test_name} tpl_shared ${TPL_LINK_LIBS} gtest gtest_main -lpthread)
    set_target_properties(${test_name} PROPERTIES ENABLE_EXPORTS true)
    add_dependencies(check ${test_name})

    # Register as a test
    add_test(NAME ${test_name}
             COMMAND ${test_name} --gtest_color=yes
             WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
    set_tests_properties(${test_name} PROPERTIES TIMEOUT 6)
endforeach()
