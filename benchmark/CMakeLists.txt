###############################################
# Benchmark library
###############################################

if (TPL_BUILD_BENCHMARKS)
    enable_testing()

    add_custom_target(runbenchmarks
            COMMAND ${CMAKE_CTEST_COMMAND} ${CTEST_FLAGS} -C benchmark -L benchmark --verbose)

    # Collect all benchmark sources
    set(benchmark_main "${PROJECT_SOURCE_DIR}/benchmark/benchmark_main.cpp")
    file(GLOB_RECURSE tpl_benchmark_srs ${PROJECT_SOURCE_DIR}/benchmark/*.cpp)
    list(REMOVE_ITEM tpl_benchmark_srs ${benchmark_main})

    foreach (benchmark_src ${tpl_benchmark_srs})
        # Derive the benchmark name
        get_filename_component(benchmark_name ${benchmark_src} NAME_WE)

        # Add it as an executable, link it to all the right libraries
        add_executable(${benchmark_name} EXCLUDE_FROM_ALL ${benchmark_src} ${benchmark_main})
        target_link_libraries(${benchmark_name} ${TPL_BENCHMARK_LINK_LIBS})

        # Add a ctest that will not be run by default.
        # To run it the command `ctest -C benchmark -L benchmark` must be used.
        # (Setting -C excludes it from the default set, and setting -L excludes the
        # default tests from it). Note the target `benchmark` runs this command.
        add_test(${benchmark_name} ${benchmark_name} --benchmark_color=true CONFIGURATIONS "benchmark")
        set_tests_properties(${benchmark_name} PROPERTIES LABELS "benchmark")

        # Add the benchmark as a dependency to the 'runbenchmark' target
        add_dependencies(runbenchmarks ${benchmark_name})
    endforeach ()
endif ()
