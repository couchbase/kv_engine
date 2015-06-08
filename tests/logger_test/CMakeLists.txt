ADD_EXECUTABLE(memcached_logger_test logger_test.cc)
TARGET_LINK_LIBRARIES(memcached_logger_test gtest gtest_main mcd_util file_logger dirutils)
ADD_TEST(NAME memcached-logger-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_logger_test)
