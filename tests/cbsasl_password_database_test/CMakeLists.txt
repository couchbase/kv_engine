ADD_EXECUTABLE(cbsasl_password_database_test
               password_database_test.cc
               ${Memcached_SOURCE_DIR}/cbsasl/cbcrypto.cc
               ${Memcached_SOURCE_DIR}/include/cbsasl/cbcrypto.h
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/password_database.cc
               ${Memcached_SOURCE_DIR}/cbsasl/password_database.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h)
TARGET_LINK_LIBRARIES(cbsasl_password_database_test
                      gtest
                      gtest_main
                      cJSON
                      platform
                      ${OPENSSL_LIBRARIES})
ADD_TEST(NAME cbsasl-password-database
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_password_database_test)
