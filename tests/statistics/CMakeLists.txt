cb_add_test_executable(memcached_statistics_test
        collector_test.cc
        main.cc
        prometheus_test.cc)
kv_enable_pch(memcached_statistics_test)
cb_enable_unity_build(memcached_statistics_test)
target_link_libraries(memcached_statistics_test
        PRIVATE
                GTest::gmock
                memcached_daemon
                memcached_logger
                statistics)

add_sanitizers(memcached_statistics_test)

ADD_TEST(NAME memcached-statistics-test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_statistics_test)
