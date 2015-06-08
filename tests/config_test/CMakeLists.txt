ADD_EXECUTABLE(memcached_config_test config_test.c
  ${Memcached_SOURCE_DIR}/daemon/config_util.c
  ${Memcached_SOURCE_DIR}/daemon/config_util.h)
TARGET_LINK_LIBRARIES(memcached_config_test cJSON platform)
ADD_TEST(NAME memcached-config
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_config_test)
