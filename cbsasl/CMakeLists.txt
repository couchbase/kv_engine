add_library(cbsasl STATIC
     ${Memcached_SOURCE_DIR}/include/cbsasl/client.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/context.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/domain.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/error.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/logging.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/mechanism.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/server.h

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
     password_database.h
     pwconv.cc
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
     user.h
     util.h)

target_link_libraries(cbsasl cbcrypto mcd_util platform)
add_sanitizers(cbsasl)

# Unit tests
if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(cbsasl_utils_test strcmp_test.cc
                   scram-sha/saslprep_test.cc)
    target_link_libraries(cbsasl_utils_test cbsasl gtest gtest_main)
    add_test(NAME cbsasl_utils_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_utils_test)

    add_executable(cbsasl_pwconv_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   pwconv_test.cc)
    target_link_libraries(cbsasl_pwconv_test
                          gtest_main
                          gtest
                          cbsasl
                          mcd_util
                          platform)
    add_test(NAME cbsasl-pwconv-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_pwconv_test)
    add_sanitizers(cbsasl_pwconv_test)

    add_executable(cbsasl_password_database_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   password_database_test.cc    )
    target_link_libraries(cbsasl_password_database_test
                          cbcrypto
                          cbsasl
                          gtest
                          platform
                          ${OPENSSL_LIBRARIES})
    add_test(NAME cbsasl-password-database
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_password_database_test)
    add_sanitizers(cbsasl_password_database_test)

    add_executable(cbsasl_client_server_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   client_server_test.cc)
    target_link_libraries(cbsasl_client_server_test cbcrypto cbsasl gtest gtest_main)
    add_test(NAME cbsasl-client-server
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_client_server_test)
    add_sanitizers(cbsasl_client_server_test)

    add_executable(cbsasl_server_test sasl_server_test.cc)
    target_link_libraries(cbsasl_server_test
                          cbsasl
                          cbcrypto
                          platform
                          gtest
                          gtest_main)
    add_test(NAME cbsasl-server-sasl
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_server_test)
    add_sanitizers(cbsasl_server_test)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
