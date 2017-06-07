ADD_LIBRARY(mc_client_connection STATIC
            client_connection.h
            client_connection.cc
            client_mcbp_commands.cc
            client_mcbp_commands.h
            client_mcbp_connection.cc
            client_mcbp_connection.h)

TARGET_LINK_LIBRARIES(mc_client_connection
                      mcbp
                      cbsasl
                      memcached_rbac
                      ${COUCHBASE_NETWORK_LIBS}
                      ${OPENSSL_LIBRARIES})
