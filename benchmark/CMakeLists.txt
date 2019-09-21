###############################################
# Benchmark library
###############################################

if (TPL_BUILD_BENCHMARKS)
    # Collect all benchmark sources
    file(GLOB_RECURSE tpl_benchmark_srs ${PROJECT_SOURCE_DIR}/benchmark/*.cpp)

    foreach (benchmark_src ${tpl_benchmark_srs})
        # Derive the benchmark name
        get_filename_component(benchmark_name ${benchmark_src} NAME_WE)

        # Add it as an executable, link it to all the right libraries
        add_executable(${benchmark_name} EXCLUDE_FROM_ALL ${benchmark_src})
        target_link_libraries(${benchmark_name} ${TPL_BENCHMARK_LINK_LIBS})

        # Add the benchmark as a dependency to the 'runbenchmark' target
        add_dependencies(runbenchmark ${benchmark_name})
        set_tests_properties(${BENCHMARK_NAME} PROPERTIES LABELS "benchmark")
    endforeach ()
endif ()
