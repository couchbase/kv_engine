ADD_EXECUTABLE(cbsasladm cbsasladm.c)
TARGET_LINK_LIBRARIES(cbsasladm mcutils mcd_util platform ${OPENSSL_LIBRARIES}
                                ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS cbsasladm RUNTIME DESTINATION bin)
