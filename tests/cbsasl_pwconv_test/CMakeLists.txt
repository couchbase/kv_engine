ADD_EXECUTABLE(cbsasl_pwconv_test
               pwconv_test.cc

               ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/password_database.cc
               ${Memcached_SOURCE_DIR}/cbsasl/password_database.h
               ${Memcached_SOURCE_DIR}/cbsasl/plain/check_password.cc
               ${Memcached_SOURCE_DIR}/cbsasl/plain/check_password.h
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h
               ${Memcached_SOURCE_DIR}/cbsasl/strcmp.cc
               )
TARGET_LINK_LIBRARIES(cbsasl_pwconv_test
                      gtest_main
                      gtest
                      cbsasl
                      platform
                      cJSON)
ADD_TEST(NAME cbsasl-pwconv-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_pwconv_test)
