ADD_EXECUTABLE(memcached_executor_test
               executor_test.cc)
TARGET_LINK_LIBRARIES(memcached_executor_test memcached_daemon gtest gmock)
add_sanitizers(memcached_executor_test)

ADD_TEST(NAME memcached-executor-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_executor_test)
SET_TESTS_PROPERTIES(memcached-executor-tests PROPERTIES TIMEOUT 60)
