# Setup includes
include_directories("${PROJECT_SOURCE_DIR}/src/include/" "${PROJECT_BINARY_DIR}")

# Setup sources
file(GLOB_RECURSE tpl_sources "${PROJECT_SOURCE_DIR}/src/*.cpp")
list(REMOVE_ITEM tpl_sources ${PROJECT_SOURCE_DIR}/src/tpl.cpp)

###############################################
# TPL library
###############################################
add_library(tpl_shared SHARED ${tpl_sources})

###############################################
# TPL executable
###############################################
set(tpl_main "${PROJECT_SOURCE_DIR}/src/tpl.cpp")
add_executable(tpl ${tpl_main})
target_link_libraries(tpl tpl_shared)