ADD_EXECUTABLE(memcached_config_test config_test.c
  ${Memcached_SOURCE_DIR}/daemon/config_util.c
  ${Memcached_SOURCE_DIR}/daemon/config_util.h)
TARGET_LINK_LIBRARIES(memcached_config_test cJSON)
ADD_TEST(memcached-config memcached_config_test)
