ADD_EXECUTABLE(cbcrypto_test cbcrypto_test.cc
               ${Memcached_SOURCE_DIR}/cbsasl/cbcrypto.cc
               ${Memcached_SOURCE_DIR}/include/cbsasl/cbcrypto.h)

TARGET_LINK_LIBRARIES(cbcrypto_test
                      platform
                      cJSON
                      gtest gtest_main
                      ${OPENSSL_LIBRARIES})

ADD_TEST(NAME cbcrypto-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbcrypto_test)
