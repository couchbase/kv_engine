ADD_EXECUTABLE(cbsasl_strcmp_test strcmp_test.c
  ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
  ${Memcached_SOURCE_DIR}/include/cbsasl/visibility.h
  ${Memcached_SOURCE_DIR}/cbsasl/strcmp.c)
TARGET_LINK_LIBRARIES(cbsasl_strcmp_test platform)
ADD_TEST(cbsasl-strcmp cbsasl_strcmp_test)
