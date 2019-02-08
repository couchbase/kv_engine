add_library(mcutils STATIC
            hostname_utils.cc hostname_utils.h)
target_link_libraries(mcutils mcbp cbsasl ${COUCHBASE_NETWORK_LIBS})

add_library(getpass STATIC
            getpass.cc getpass.h)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(memcached-hostutils-test
                   hostname_utils_test.cc)
    target_link_libraries(memcached-hostutils-test
                          mcutils
                          platform
                          gtest
                          gtest_main)
    add_test(NAME memcached-hostutils-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached-hostutils-test)

    add_subdirectory(engine_testapp)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)

add_subdirectory(mcctl)
add_subdirectory(mclogsplit)
add_subdirectory(mcstat)
add_subdirectory(mctimings)
add_subdirectory(mctrace)
