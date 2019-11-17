# Main libcount library
add_library(libcount
        STATIC
        ${CMAKE_CURRENT_SOURCE_DIR}/c.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/empirical_data.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/hll.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/hll_data.cc)

set_target_properties(libcount PROPERTIES OUTPUT_NAME "count")
target_include_directories(libcount
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/../include)

# Empiral data test
add_executable(empirical_data_test ${CMAKE_CURRENT_SOURCE_DIR}/empirical_data_test.cc)
target_include_directories(empirical_data_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(empirical_data_test LINK_PUBLIC libcount)