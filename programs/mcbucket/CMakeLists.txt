ADD_EXECUTABLE(mcbucket mcbucket.cc)
TARGET_LINK_LIBRARIES(mcbucket mc_client_connection platform
                               ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
