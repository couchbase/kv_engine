ADD_EXECUTABLE(cbsasl_hmac_test hmac_test.c
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/hmac.c
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/hmac.h
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/md5.c
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/md5.h)
TARGET_LINK_LIBRARIES(cbsasl_hmac_test platform)
ADD_TEST(cbsasl-hmac cbsasl_hmac_test)
