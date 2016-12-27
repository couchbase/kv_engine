IF (MEMCACHED_USE_GREENSTACK)
    LIST(APPEND MEMCACHED_GREENSTACK_LIBS greenstack)
    LIST(APPEND MEMCACHED_GREENSTACK_SRC
         client_greenstack_connection.cc
         client_greenstack_connection.h)
ENDIF (MEMCACHED_USE_GREENSTACK)

ADD_LIBRARY(mc_client_connection STATIC
            ${MEMCACHED_GREENSTACK_SRC}
            client_connection.h
            client_connection.cc
            client_mcbp_commands.cc
            client_mcbp_commands.h
            client_mcbp_connection.cc
            client_mcbp_connection.h)

TARGET_LINK_LIBRARIES(mc_client_connection
                      mcbp
                      cbsasl
                      ${MEMCACHED_GREENSTACK_LIBS}
                      ${COUCHBASE_NETWORK_LIBS}
                      ${OPENSSL_LIBRARIES})
