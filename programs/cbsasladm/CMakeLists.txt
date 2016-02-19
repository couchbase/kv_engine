ADD_EXECUTABLE(cbsasladm
               cbsasladm.cc
               ${Memcached_SOURCE_DIR}/cbsasl/cbcrypto.cc
               ${Memcached_SOURCE_DIR}/cbsasl/cbcrypto.h
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h)
TARGET_LINK_LIBRARIES(cbsasladm mcutils mcd_util platform ${OPENSSL_LIBRARIES}
                                ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS cbsasladm RUNTIME DESTINATION bin)
