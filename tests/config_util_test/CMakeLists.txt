ADD_EXECUTABLE(memcached_config_util_test config_util_test.cc)
TARGET_LINK_LIBRARIES(memcached_config_util_test
                      memcached_daemon
                      gtest gtest_main)
ADD_SANITIZERS(memcached_config_util_test)
ADD_TEST(NAME memcached-config
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_config_util_test)
