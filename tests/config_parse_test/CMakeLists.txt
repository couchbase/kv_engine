ADD_EXECUTABLE(memcached_config_parse_test config_parse_test.cc)

TARGET_LINK_LIBRARIES(memcached_config_parse_test
                      memcached_daemon
                      gtest)
ADD_SANITIZERS(memcached_config_parse_test)
ADD_TEST(NAME memcache-config-parse
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_config_parse_test)

IF (ENABLE_DTRACE)
    ADD_DEPENDENCIES(memcached_config_parse_test generate_memcached_dtrace_h)
ENDIF (ENABLE_DTRACE)
