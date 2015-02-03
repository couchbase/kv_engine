ADD_EXECUTABLE(cbsasladm cbsasladm.c)
TARGET_LINK_LIBRARIES(cbsasladm mcutils platform ${OPENSSL_LIBRARIES}
                                ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS cbsasladm RUNTIME DESTINATION bin)
