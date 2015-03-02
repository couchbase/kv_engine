ADD_EXECUTABLE(cbsasl_client_server_test cbsasl_test.c
  ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
  ${Memcached_SOURCE_DIR}/include/cbsasl/visibility.h)
TARGET_LINK_LIBRARIES(cbsasl_client_server_test cbsasl)
ADD_TEST(cbsasl-client-server cbsasl_client_server_test)
