ADD_EXECUTABLE(cbsasl_hmac_test hmac_test.cc)
TARGET_LINK_LIBRARIES(cbsasl_hmac_test platform gtest gtest_main ${OPENSSL_LIBRARIES})
ADD_TEST(NAME cbsasl-hmac
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_hmac_test)
