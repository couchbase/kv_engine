ADD_EXECUTABLE(eventtest event.cc)
TARGET_LINK_LIBRARIES(eventtest cbsocket platform ${LIBEVENT_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})
