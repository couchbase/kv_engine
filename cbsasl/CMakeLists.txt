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
                fmt::fmt
                phosphor
                mcd_util
                ${OPENSSL_LIBRARIES})
add_sanitizers(cbsasl)

if (LIBSODIUM_INCLUDE_DIR AND LIBSODIUM_LIBRARIES)
    message(STATUS "Adding support for Argon2id v1.3")
    target_include_directories(cbsasl SYSTEM AFTER PRIVATE ${LIBSODIUM_INCLUDE_DIR})
    target_compile_definitions(cbsasl PUBLIC -DHAVE_LIBSODIUM=1)
    target_link_libraries(cbsasl PRIVATE ${LIBSODIUM_LIBRARIES})
endif()

cb_add_test_executable(cbsasl_unit_test
                       cbcrypto_test.cc
                       client_server_test.cc
                       password_database_test.cc
                       plain/plain_test.cc
                       sasl_server_test.cc
                       scram-sha/saslprep_test.cc
                       scram-sha/scram-sha_test.cc
                       strcmp_test.cc
                       user_test.cc)
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
