# Build examples?
if (${LIBCOUNT_BUILD_EXAMPLES})
    find_package(OpenSSL)

    # c_example
    add_executable(c_example ${CMAKE_CURRENT_SOURCE_DIR}/c_example.c)
    target_include_directories(c_example
            PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..
            PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/../include)
    target_link_libraries(c_example LINK_PUBLIC count ${OPENSSL_LIBRARIES})

    # cc_example
    add_executable(cc_example ${CMAKE_CURRENT_SOURCE_DIR}/cc_example.cc)
    target_include_directories(cc_example
            PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..
            PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/../include)
    target_link_libraries(cc_example LINK_PUBLIC count ${OPENSSL_LIBRARIES})

    # merge_example
    add_executable(merge_example ${CMAKE_CURRENT_SOURCE_DIR}/merge_example.cc)
    target_include_directories(merge_example
            PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..
            PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/../include)
    target_link_libraries(merge_example LINK_PUBLIC count ${OPENSSL_LIBRARIES})
endif()

# Build certification?
if (${LIBCOUNT_BUILD_CERTIFY})
    find_package(OpenSSL)

    # certify
    add_executable(certify ${CMAKE_CURRENT_SOURCE_DIR}/certify.cc)
    target_include_directories(certify
            PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/..
            PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/../include)
    target_link_libraries(certify LINK_PUBLIC count ${OPENSSL_LIBRARIES})

    # Run the 'certify' binary
    add_custom_target(check-certify
            DEPENDS certify
            COMMAND ${CMAKE_BINARY_DIR}/examples/certify)
endif()