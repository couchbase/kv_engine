add_library(cbsasl STATIC
            cbcrypto.cc
            client.cc
            context.cc
            domain.cc
            log.cc
            log_callback.cc
            mechanism.cc
            plain/check_password.cc
            plain/check_password.h
            plain/plain.cc
            plain/plain.h
            password_database.cc
            pwfile.cc
            pwfile.h
            scram-sha/scram-sha.cc
            scram-sha/scram-sha.h
            scram-sha/stringutils.cc
            scram-sha/stringutils.h
            server.cc
            strcmp.cc
            strerror.cc
            user.cc
            util.h)
kv_enable_pch(cbsasl)
cb_enable_unity_build(cbsasl)

target_link_libraries(cbsasl
        PUBLIC
                platform
        PRIVATE
                mcd_util)
add_sanitizers(cbsasl)

if (APPLE)
    # Apple is using the Common Crypto Framework for all crypto functions
    target_link_libraries(cbsasl PRIVATE platform phosphor)
elseif (WIN32)
    # We're using Bcrypt on windows
    target_link_libraries(cbsasl PRIVATE platform phosphor Bcrypt)
else()
    # Use the OpenSSL ones for all other platforms
    target_link_libraries(cbsasl PRIVATE platform phosphor ${OPENSSL_LIBRARIES})
endif ()

cb_add_test_executable(cbsasl_unit_test
                       cbcrypto_test.cc
                       client_server_test.cc
                       password_database_test.cc
                       sasl_server_test.cc
                       scram-sha/saslprep_test.cc
                       strcmp_test.cc)
cb_enable_unity_build(cbsasl_unit_test)
kv_enable_pch(cbsasl_unit_test)
target_link_libraries(cbsasl_unit_test
        PRIVATE
                cbsasl
                GTest::gtest
                GTest::gtest_main
                platform)
add_test(NAME cbsasl_unit_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_unit_test)
add_sanitizers(cbsasl_unit_test)
