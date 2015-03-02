ADD_EXECUTABLE(cbsasl_pwfile_test pwfile_test.c
  ${Memcached_SOURCE_DIR}/cbsasl/pwfile.c
  ${Memcached_SOURCE_DIR}/cbsasl/pwfile.h
  ${Memcached_SOURCE_DIR}/cbsasl/hash.c
  ${Memcached_SOURCE_DIR}/cbsasl/hash.h)
TARGET_LINK_LIBRARIES(cbsasl_pwfile_test platform)
ADD_TEST(cbsasl-pwfile cbsasl_pwfile_test)
