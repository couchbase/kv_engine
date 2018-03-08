list(APPEND CBSASL_SOURCES
     ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/saslauthd_config.h
     ${Memcached_SOURCE_DIR}/cbsasl/cbsasl_internal.h
     ${Memcached_SOURCE_DIR}/cbsasl/client.cc
     ${Memcached_SOURCE_DIR}/cbsasl/common.cc
     ${Memcached_SOURCE_DIR}/cbsasl/log.cc
     ${Memcached_SOURCE_DIR}/cbsasl/log_callback.cc
     ${Memcached_SOURCE_DIR}/cbsasl/mechanismfactory.cc
     ${Memcached_SOURCE_DIR}/cbsasl/mechanismfactory.h
     ${Memcached_SOURCE_DIR}/cbsasl/plain/check_password.cc
     ${Memcached_SOURCE_DIR}/cbsasl/plain/check_password.h
     ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.cc
     ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.h
     ${Memcached_SOURCE_DIR}/cbsasl/password_database.cc
     ${Memcached_SOURCE_DIR}/cbsasl/password_database.h
     ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
     ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
     ${Memcached_SOURCE_DIR}/cbsasl/pwfile.cc
     ${Memcached_SOURCE_DIR}/cbsasl/pwfile.h
     ${Memcached_SOURCE_DIR}/cbsasl/saslauthd_config.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/scram-sha.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/scram-sha.h
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.h
     ${Memcached_SOURCE_DIR}/cbsasl/server.cc
     ${Memcached_SOURCE_DIR}/cbsasl/strcmp.cc
     ${Memcached_SOURCE_DIR}/cbsasl/strerror.cc
     ${Memcached_SOURCE_DIR}/cbsasl/user.cc
     ${Memcached_SOURCE_DIR}/cbsasl/user.h
     ${Memcached_SOURCE_DIR}/cbsasl/util.h)

if (NOT WIN32)
    list(APPEND CBSASL_SOURCES
         ${Memcached_SOURCE_DIR}/cbsasl/plain/saslauthd.cc
         ${Memcached_SOURCE_DIR}/cbsasl/plain/saslauthd.h)
endif (NOT WIN32)

add_library(cbsasl SHARED ${CBSASL_SOURCES})
generate_export_header(cbsasl
                       EXPORT_MACRO_NAME CBSASL_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/cbsasl/visibility.h)
set_target_properties(cbsasl PROPERTIES SOVERSION 1.1.1)
target_link_libraries(cbsasl cbcrypto cbsocket dirutils mcd_util cJSON platform)

install(TARGETS cbsasl
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)


ADD_EXECUTABLE(cbsasladm
               cbsasladm.cc
               log.cc
               pwconv.cc
               pwconv.h
               user.cc
               user.h)
TARGET_LINK_LIBRARIES(cbsasladm
                      cbsasl
                      cbsocket
                      mc_client_connection
                      mcd_util
                      mcutils
                      platform)
INSTALL(TARGETS cbsasladm RUNTIME DESTINATION bin)



# Unit tests
if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    ADD_EXECUTABLE(cbsasl_strcmp_test strcmp_test.cc
                   strcmp.cc
                   util.h)
    TARGET_LINK_LIBRARIES(cbsasl_strcmp_test gtest gtest_main)
    ADD_TEST(NAME cbsasl-strcmp
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_strcmp_test)

    ADD_EXECUTABLE(cbsasl_pwconv_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   log.cc
                   password_database.cc
                   password_database.h
                   plain/check_password.cc
                   plain/check_password.h
                   pwconv.cc
                   pwconv.h
                   pwconv_test.cc
                   strcmp.cc
                   user.cc
                   user.h)
    TARGET_LINK_LIBRARIES(cbsasl_pwconv_test
                          gtest_main
                          gtest
                          cbsasl
                          mcd_util
                          platform
                          cJSON)
    ADD_TEST(NAME cbsasl-pwconv-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_pwconv_test)

    ADD_EXECUTABLE(cbsasl_password_database_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   log.cc
                   password_database.cc
                   password_database.h
                   password_database_test.cc
                   pwconv.cc
                   pwconv.h
                   user.cc
                   user.h)
    TARGET_LINK_LIBRARIES(cbsasl_password_database_test
                          cbcrypto
                          gtest
                          cJSON
                          dirutils
                          platform
                          ${OPENSSL_LIBRARIES})
    ADD_TEST(NAME cbsasl-password-database
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_password_database_test)

    ADD_EXECUTABLE(cbsasl_client_server_test
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h
                   ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
                   client_server_test.cc)


    TARGET_LINK_LIBRARIES(cbsasl_client_server_test cbcrypto cbsasl gtest gtest_main)
    ADD_TEST(NAME cbsasl-client-server
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbsasl_client_server_test)

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
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
