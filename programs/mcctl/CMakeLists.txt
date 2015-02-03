ADD_EXECUTABLE(mcctl mcctl.c)
TARGET_LINK_LIBRARIES(mcctl mcutils platform mcd_util
                            ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS mcctl RUNTIME DESTINATION bin)
