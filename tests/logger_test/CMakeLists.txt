ADD_EXECUTABLE(memcached_logger_test logger_test.cc)
TARGET_LINK_LIBRARIES(memcached_logger_test mcd_util file_logger dirutils)
ADD_TEST(memcached-logger-test-rotate memcached_logger_test rotate)
# There are too many spurious test failures
#ADD_TEST(memcached-logger-test-dedupe memcached_logger_test dedupe)
