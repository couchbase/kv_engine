ADD_EXECUTABLE(mcsend common.cc common.h mcsend.cc)
TARGET_LINK_LIBRARIES(mcsend
                      mcutils
                      mcd_util
                      platform
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})

ADD_EXECUTABLE(mcrecv common.cc common.h mcrecv.cc)
TARGET_LINK_LIBRARIES(mcrecv
                      mcutils
                      mcd_util
                      platform
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS})

INSTALL(TARGETS mcrecv mcsend RUNTIME DESTINATION bin)
