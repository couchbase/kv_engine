ADD_EXECUTABLE(mctimings mctimings.cc)
TARGET_LINK_LIBRARIES(mctimings mcutils mcd_util cJSON platform
                                ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
INSTALL(TARGETS mctimings RUNTIME DESTINATION bin)
