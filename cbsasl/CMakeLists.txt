add_library(cbsasl STATIC
            client.cc
            context.cc
            domain.cc
            mechanism.cc
            oauthbearer/oauthbearer.cc
            oauthbearer/oauthbearer.h
            plain/check_password.cc
            plain/check_password.h
            plain/plain.cc
            plain/plain.h
            password_database.cc
            pwfile.cc
            pwfile.h
            scram_password_meta_data.cc
            scram-sha/scram-sha-client.cc
            scram-sha/scram-sha-server.cc
            scram-sha/scram-sha.cc
            scram-sha/scram-sha.h
            scram-sha/stringutils.cc
            scram-sha/stringutils.h
            server.cc
            strcmp.cc
            strerror.cc
            user.cc
            username_util.cc
            username_util.h
            util.h)
kv_enable_pch(cbsasl)
cb_enable_unity_build(cbsasl)

target_link_libraries(cbsasl
        PUBLIC
                platform
        PRIVATE
                mcd_dek
                cbcrypto
                fmt::fmt
                phosphor
                mcd_util
                memcached_logger
                libsodium::libsodium)
add_sanitizers(cbsasl)

cb_add_test_executable(cbsasl_unit_test
                       client_server_test.cc
                       domain_test.cc
                       password_database_test.cc
                       plain/plain_test.cc
                       sasl_server_test.cc
                       scram-sha/saslprep_test.cc
                       scram-sha/scram-sha_test.cc
                       strcmp_test.cc
                       user_test.cc
                       username_util_test.cc)
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
