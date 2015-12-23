# We're using an "internal api" of the library in the test so we can't
# use the library we just built
ADD_EXECUTABLE(cbsasl_server_test sasl_server_test.c ${CBSASL_SOURCES})
SET_TARGET_PROPERTIES(cbsasl_server_test PROPERTIES COMPILE_FLAGS -DBUILDING_CBSASL=1)
TARGET_LINK_LIBRARIES(cbsasl_server_test platform ${OPENSSL_LIBRARIES})
ADD_TEST(NAME cbsasl-server-sasl
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_server_test)
