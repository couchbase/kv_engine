ADD_EXECUTABLE(config_parse_test config_parse_test.c
  ${Memcached_SOURCE_DIR}/daemon/config_util.c
  ${Memcached_SOURCE_DIR}/daemon/config_util.h
  ${Memcached_SOURCE_DIR}/daemon/cmdline.h
  ${Memcached_SOURCE_DIR}/daemon/cmdline.c
  ${Memcached_SOURCE_DIR}/utilities/util.c)
TARGET_LINK_LIBRARIES(config_parse_test cJSON platform ${COUCHBASE_NETWORK_LIBS})
ADD_TEST(memcache-config-parse config_parse_test)
IF(ENABLE_DTRACE)
  ADD_DEPENDENCIES(config_parse_test generate_memcached_dtrace_h)
ENDIF(ENABLE_DTRACE)
