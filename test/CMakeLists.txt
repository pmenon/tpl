enable_testing()

###############################################
# Setup GoogleTest
###############################################
include_directories(${PROJECT_SOURCE_DIR}/third_party/gtest/include
                    ${PROJECT_SOURCE_DIR}/third_party/spdlog/include)
link_directories(${PROJECT_SOURCE_DIR}/third_party/gtest/lib)

###############################################
# Find all test sources, and find ALL headers
###############################################
file(GLOB_RECURSE tpl_test_srs "${PROJECT_SOURCE_DIR}/test/*.cpp")

include_directories("${PROJECT_SOURCE_DIR}/test/include" "${PROJECT_SOURCE_DIR}/src/include")

###############################################
# Setup 'make check' target. We'll add each
# test as a dependency to the target below
###############################################
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} ${CTEST_FLAGS} --verbose)

# From list of test files we'll create tests test_name.cpp -> test_name
foreach(test_src ${tpl_test_srs})
    # Get the name of the test from the name
    get_filename_component(test_name ${test_src} NAME_WE)

    # Build a binary for it
    add_executable(${test_name} EXCLUDE_FROM_ALL ${test_src})
    add_dependencies(check ${test_name})
    target_link_libraries(${test_name} gtest gtest_main tpl_shared ${TPL_LINK_LIBS} ${CMAKE_THREAD_LIBS_INIT} -lpthread)

    # Register as a test
    add_test(${test_name} ${test_name} --gtest_color=yes)
    set_tests_properties(${test_name} PROPERTIES TIMEOUT 5)
endforeach()