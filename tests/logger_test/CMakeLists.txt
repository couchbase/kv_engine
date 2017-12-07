ADD_EXECUTABLE(memcached_logger_test
               ${Memcached_SOURCE_DIR}/extensions/loggers/file_logger_utilities.h
               ${Memcached_SOURCE_DIR}/extensions/loggers/file_logger_utilities.cc
               logger_test_common.cc
               logger_test.cc)
TARGET_LINK_LIBRARIES(memcached_logger_test gtest gtest_main mcd_util file_logger dirutils)
ADD_TEST(NAME memcached-logger-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_logger_test)

IF (NOT WIN32)
   # Test makes use of {get,set}rlimit() which don't exist on Windows.
   ADD_EXECUTABLE(memcached_logger_emfile_test
                  logger_test_common.cc
                  logger_emfile_test.cc)
   TARGET_LINK_LIBRARIES(memcached_logger_emfile_test mcd_util file_logger dirutils)
   ADD_TEST(NAME memcached-logger-emfile-test
            WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
            COMMAND memcached_logger_emfile_test)
ENDIF (NOT WIN32)


ADD_EXECUTABLE(memcached_spdlogger_test
               logger_test_common.cc
               spdlogger_test.cc
               ${Memcached_SOURCE_DIR}/extensions/loggers/custom_rotating_file_sink.h
               ${Memcached_SOURCE_DIR}/extensions/loggers/custom_rotating_file_sink.cc
               ${Memcached_SOURCE_DIR}/extensions/loggers/file_logger_utilities.cc
               ${Memcached_SOURCE_DIR}/extensions/loggers/file_logger_utilities.h)
TARGET_LINK_LIBRARIES(memcached_spdlogger_test gtest gtest_main mcd_util spdlogger dirutils)
ADD_TEST(NAME memcached-spdlogger-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_spdlogger_test)
