ADD_EXECUTABLE(cbsasl_server_test sasl_server_test.cc)
TARGET_LINK_LIBRARIES(cbsasl_server_test
                      cbsasl
                      cbcrypto
                      platform
                      cJSON
                      gtest
                      gtest_main)
ADD_TEST(NAME cbsasl-server-sasl
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_server_test)
