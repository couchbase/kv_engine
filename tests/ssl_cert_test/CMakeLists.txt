ADD_EXECUTABLE(memcached_cert_test ssl_cert_test.cc
               ${Memcached_SOURCE_DIR}/daemon/settings.cc
               ${Memcached_SOURCE_DIR}/daemon/settings.h
               ${Memcached_SOURCE_DIR}/daemon/sslcert.cc
               ${Memcached_SOURCE_DIR}/daemon/sslcert.h
               ${Memcached_SOURCE_DIR}/daemon/ssl_utils.cc)

TARGET_LINK_LIBRARIES(memcached_cert_test
                      cJSON
                      mcbp
                      platform
                      dirutils
                      gtest gtest_main
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})
ADD_TEST(NAME memcached_cert_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_cert_test)

IF (ENABLE_DTRACE)
    ADD_DEPENDENCIES(memcached_cert_test generate_memcached_dtrace_h)
ENDIF (ENABLE_DTRACE)
