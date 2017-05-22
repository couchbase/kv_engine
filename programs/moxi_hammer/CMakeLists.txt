add_executable(moxi_hammer moxi_hammer.cc)
target_link_libraries(moxi_hammer
                      platform
                      ${LIBEVENT_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})
