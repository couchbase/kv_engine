ADD_EXECUTABLE(ssltest ssltest.cc)
TARGET_LINK_LIBRARIES(ssltest mcd_util platform cbsasl mcutils
                      ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
