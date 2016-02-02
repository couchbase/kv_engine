ADD_EXECUTABLE(ssltest ssltest.c
  ${Memcached_SOURCE_DIR}/programs/utilities.cc
  ${Memcached_SOURCE_DIR}/programs/utilities.h)
TARGET_LINK_LIBRARIES(ssltest mcd_util platform cbsasl
                      ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
