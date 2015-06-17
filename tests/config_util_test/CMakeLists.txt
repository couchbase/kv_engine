ADD_EXECUTABLE(memcached_config_util_test config_util_test.cc
  ${Memcached_SOURCE_DIR}/daemon/config_util.c
  ${Memcached_SOURCE_DIR}/daemon/config_util.h)
TARGET_LINK_LIBRARIES(memcached_config_util_test cJSON platform gtest gtest_main)
ADD_TEST(NAME memcached-config
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_config_util_test)
