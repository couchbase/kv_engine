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
kv_enable_pch(cbsasl)
cb_enable_unity_build(cbsasl)

target_link_libraries(cbsasl
        PUBLIC
                platform
        PRIVATE
                cbcrypto
                mcd_util)
add_sanitizers(cbsasl)

cb_add_test_executable(cbsasl_unit_test
                       client_server_test.cc
                       password_database_test.cc
                       pwconv_test.cc
                       sasl_server_test.cc
                       scram-sha/saslprep_test.cc
                       strcmp_test.cc)
cb_enable_unity_build(cbsasl_unit_test)
kv_enable_pch(cbsasl_unit_test)
target_link_libraries(cbsasl_unit_test
        PRIVATE
                cbsasl
                cbcrypto
                gtest
                gtest_main
                platform)
add_test(NAME cbsasl_unit_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_unit_test)
add_sanitizers(cbsasl_unit_test)
