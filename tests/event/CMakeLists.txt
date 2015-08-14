ADD_EXECUTABLE(eventtest event.cc)
TARGET_LINK_LIBRARIES(eventtest platform ${LIBEVENT_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})
