INCLUDE_DIRECTORIES(AFTER SYSTEM ${gtest_SOURCE_DIR}/include)
cb_add_test_executable(memcached-engine-error-test engine_error_test.cc)
kv_enable_pch(memcached-engine-error-test)
target_link_libraries(memcached-engine-error-test
        PRIVATE Folly::headers engine_utilities gtest gtest_main)
ADD_TEST(NAME memcached-engine-error-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-engine-error-test)
