ADD_EXECUTABLE(config_parse_test config_parse_test.cc
  ${Memcached_SOURCE_DIR}/daemon/config_util.cc
  ${Memcached_SOURCE_DIR}/daemon/config_util.h
  ${Memcached_SOURCE_DIR}/daemon/cmdline.h
  ${Memcached_SOURCE_DIR}/daemon/cmdline.cc
  ${Memcached_SOURCE_DIR}/utilities/util.c)
TARGET_LINK_LIBRARIES(config_parse_test cJSON JSON_checker
        platform ${COUCHBASE_NETWORK_LIBS})
ADD_TEST(NAME memcache-config-parse
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND config_parse_test)
IF(ENABLE_DTRACE)
  ADD_DEPENDENCIES(config_parse_test generate_memcached_dtrace_h)
ENDIF(ENABLE_DTRACE)
