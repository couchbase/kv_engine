ADD_EXECUTABLE(cbsasl_pwfile_test pwfile_test.cc
               ${Memcached_SOURCE_DIR}/cbsasl/cbcrypto.cc
               ${Memcached_SOURCE_DIR}/cbsasl/cbcrypto.h
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/password_database.cc
               ${Memcached_SOURCE_DIR}/cbsasl/password_database.h
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
               ${Memcached_SOURCE_DIR}/cbsasl/pwfile.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwfile.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h)
TARGET_LINK_LIBRARIES(cbsasl_pwfile_test gtest gtest_main platform cJSON
                      ${OPENSSL_LIBRARIES})
ADD_TEST(NAME cbsasl-pwfile
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_pwfile_test)
