ADD_EXECUTABLE(mcstat mcstat.c)
TARGET_LINK_LIBRARIES(mcstat mcutils platform ${OPENSSL_LIBRARIES}
                             ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS mcstat RUNTIME DESTINATION bin)
