add_library(mc_client_connection STATIC
            client_connection.h
            client_connection.cc
            client_connection_map.cc
            client_connection_map.h
            client_mcbp_commands.cc
            client_mcbp_commands.h
            frameinfo.cc frameinfo.h)
kv_enable_pch(mc_client_connection)
cb_enable_unity_build(mc_client_connection)

target_link_libraries(mc_client_connection PRIVATE
  PUBLIC
    Folly::headers
    mcbp
    cbsasl
    cbsocket
    mcd_tracing
    memcached_rbac
    platform
    ${COUCHBASE_NETWORK_LIBS}
    ${OPENSSL_LIBRARIES}
  PRIVATE
    cbcompress)
