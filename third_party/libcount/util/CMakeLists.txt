#
# Data generator
#

add_executable(mkdata
        ${CMAKE_CURRENT_SOURCE_DIR}/raw_data.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/mkdata.cc)
target_include_directories(mkdata
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(mkdata LINK_PUBLIC)