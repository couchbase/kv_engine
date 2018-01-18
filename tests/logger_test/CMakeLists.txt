ADD_EXECUTABLE(memcached_spdlogger_test
               logger_test_common.cc
               spdlogger_test.cc
               ${Memcached_SOURCE_DIR}/extensions/loggers/custom_rotating_file_sink.h
               ${Memcached_SOURCE_DIR}/extensions/loggers/custom_rotating_file_sink.cc
               ${Memcached_SOURCE_DIR}/extensions/loggers/file_logger_utilities.cc
               ${Memcached_SOURCE_DIR}/extensions/loggers/file_logger_utilities.cc)
TARGET_LINK_LIBRARIES(memcached_spdlogger_test gtest gtest_main mcd_util spdlogger dirutils)
ADD_TEST(NAME memcached-spdlogger-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_spdlogger_test)
