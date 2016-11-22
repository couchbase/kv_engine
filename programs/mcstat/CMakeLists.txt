ADD_EXECUTABLE(mcstat mcstat.cc)
TARGET_LINK_LIBRARIES(mcstat mcutils mcd_util platform ${OPENSSL_LIBRARIES}
                             ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS mcstat RUNTIME DESTINATION bin)
