ADD_EXECUTABLE(mchello mchello.c)
TARGET_LINK_LIBRARIES(mchello mcutils mcd_util platform
                              ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
