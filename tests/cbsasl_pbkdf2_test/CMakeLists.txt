# For some reason I got a compile error on windows caused by mixing
# the order of some of the windows headers. Given that this program
# doesn't test much (apart from printing out the time it takes to
# generate the HMAC's with various sha's and iteration counts we can
# just ignore it for now
IF (NOT WIN32)
    ADD_EXECUTABLE(cbsasl_pbkdf2_test cbsasl_pbkdf2_test.cc)
    TARGET_LINK_LIBRARIES(cbsasl_pbkdf2_test gtest gtest_main platform
                          ${OPENSSL_LIBRARIES})
    ADD_TEST(NAME cbsasl-pbkdf2
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_pbkdf2_test)
ENDIF (NOT WIN32)
