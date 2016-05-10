ADD_EXECUTABLE(cbcrypto_test cbcrypto_test.cc
               ${Memcached_SOURCE_DIR}/cbsasl/cbcrypto.cc
               ${Memcached_SOURCE_DIR}/include/cbsasl/cbcrypto.h)

IF (NOT APPLE)
    LIST(APPEND CBCRYPTO_TEST_LIBS ${OPENSSL_LIBRARIES})
ENDIF (NOT APPLE)

TARGET_LINK_LIBRARIES(cbcrypto_test ${CBCRYPTO_TEST_LIBS}
                      platform gtest gtest_main)

ADD_TEST(NAME cbcrypto-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbcrypto_test)
