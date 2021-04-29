cb_add_test_executable(memcached_statistics_test
        prometheus_test.cc)
kv_enable_pch(memcached_statistics_test)
TARGET_LINK_LIBRARIES(memcached_statistics_test statistics memcached_daemon gtest_main gmock)

add_sanitizers(memcached_statistics_test)

ADD_TEST(NAME memcached-statistics-test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_statistics_test)
