ADD_EXECUTABLE(mcflush mcflush.cc)
TARGET_LINK_LIBRARIES(mcflush mcutils mcd_util platform
                      ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
