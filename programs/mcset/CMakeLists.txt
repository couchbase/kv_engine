ADD_EXECUTABLE(mcset mcset.c)
TARGET_LINK_LIBRARIES(mcset mcutils mcd_util platform ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})
