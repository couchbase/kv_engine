list(APPEND CBSASL_SOURCES
     ${Memcached_SOURCE_DIR}/include/cbsasl/client.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/context.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/domain.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/error.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/logging.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/mechanism.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/saslauthd_config.h
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
     saslauthd_config.cc
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

if (NOT WIN32)
    list(APPEND CBSASL_SOURCES
         plain/saslauthd.cc
         plain/saslauthd.h)
endif (NOT WIN32)

add_library(cbsasl SHARED ${CBSASL_SOURCES})
generate_export_header(cbsasl
                       EXPORT_MACRO_NAME CBSASL_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/cbsasl/visibility.h)
set_target_properties(cbsasl PROPERTIES SOVERSION 2.0.0)
target_link_libraries(cbsasl cbcrypto cbsocket dirutils mcd_util platform)
add_sanitizers(cbsasl)

install(TARGETS cbsasl
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

add_executable(cbsasladm cbsasladm.cc)
target_link_libraries(cbsasladm
                      cbsasl
                      cbsocket
                      mc_client_connection
                      mcd_util
                      mcutils
                      platform)
install(TARGETS cbsasladm RUNTIME DESTINATION bin)

# Unit tests
if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(cbsasl_strcmp_test strcmp_test.cc
                   strcmp.cc
                   util.h)
    target_link_libraries(cbsasl_strcmp_test gtest gtest_main)
    add_test(NAME cbsasl-strcmp
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_strcmp_test)

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

    add_executable(cbsasl_password_database_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   password_database_test.cc    )
    target_link_libraries(cbsasl_password_database_test
                          cbcrypto
                          cbsasl
                          gtest
                          dirutils
                          platform
                          ${OPENSSL_LIBRARIES})
    add_test(NAME cbsasl-password-database
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_password_database_test)

    add_executable(cbsasl_client_server_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   client_server_test.cc)
    target_link_libraries(cbsasl_client_server_test cbcrypto cbsasl gtest gtest_main)
    add_test(NAME cbsasl-client-server
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_client_server_test)

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
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
