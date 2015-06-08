ADD_EXECUTABLE(cbsasl_client_server_test cbsasl_test.c
  ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
  ${Memcached_SOURCE_DIR}/include/cbsasl/visibility.h)
TARGET_LINK_LIBRARIES(cbsasl_client_server_test cbsasl)
ADD_TEST(NAME cbsasl-client-server
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_client_server_test)
