ADD_EXECUTABLE(memcached_config_parse_test config_parse_test.cc
               ${Memcached_SOURCE_DIR}/daemon/settings.cc
               ${Memcached_SOURCE_DIR}/daemon/settings.h
               ${Memcached_SOURCE_DIR}/daemon/ssl_utils.cc)

TARGET_LINK_LIBRARIES(memcached_config_parse_test
                      cJSON
                      JSON_checker
                      platform
                      dirutils
                      gtest gtest_main
                      ${COUCHBASE_NETWORK_LIBS})
ADD_TEST(NAME memcache-config-parse
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_config_parse_test)

IF (ENABLE_DTRACE)
    ADD_DEPENDENCIES(memcached_config_parse_test generate_memcached_dtrace_h)
ENDIF (ENABLE_DTRACE)
