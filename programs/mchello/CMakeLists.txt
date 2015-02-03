ADD_EXECUTABLE(mchello mchello.c)
TARGET_LINK_LIBRARIES(mchello mcutils platform
                              ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
