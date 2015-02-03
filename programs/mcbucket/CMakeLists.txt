ADD_EXECUTABLE(mcbucket mcbucket.c)
TARGET_LINK_LIBRARIES(mcbucket mcutils mcd_util platform
                               ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS mcbucket RUNTIME DESTINATION bin)
