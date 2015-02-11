ADD_EXECUTABLE(mcstat mcstat.c)
TARGET_LINK_LIBRARIES(mcstat mcutils mcd_util platform ${OPENSSL_LIBRARIES}
                             ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS mcstat RUNTIME DESTINATION bin)
