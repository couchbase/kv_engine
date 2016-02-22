ADD_LIBRARY(mc_client_connection STATIC
            client_connection.h
            client_connection.cc
            client_greenstack_connection.cc
            client_greenstack_connection.h
            client_mcbp_connection.cc
            client_mcbp_connection.h)

TARGET_LINK_LIBRARIES(mc_client_connection
                      mcbp
                      greenstack
                      cbsasl
                      ${COUCHBASE_NETWORK_LIBS}
                      ${OPENSSL_LIBRARIES})