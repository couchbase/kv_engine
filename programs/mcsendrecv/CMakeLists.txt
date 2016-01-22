ADD_EXECUTABLE(mcsend common.cc common.h mcsend.cc)
TARGET_LINK_LIBRARIES(mcsend
                      mcd_util
                      mcutils
                      platform
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})

ADD_EXECUTABLE(mcrecv common.cc common.h mcrecv.cc)
TARGET_LINK_LIBRARIES(mcrecv
                      mcd_util
                      mcutils
                      platform
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})

INSTALL(TARGETS mcrecv mcsend RUNTIME DESTINATION bin)
