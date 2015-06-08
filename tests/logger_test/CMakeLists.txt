ADD_EXECUTABLE(memcached_logger_test logger_test.cc)
TARGET_LINK_LIBRARIES(memcached_logger_test mcd_util file_logger dirutils)
ADD_TEST(NAME memcached-logger-test-rotate
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_logger_test rotate)
# There are too many spurious test failures
#ADD_TEST(memcached-logger-test-dedupe memcached_logger_test dedupe)
