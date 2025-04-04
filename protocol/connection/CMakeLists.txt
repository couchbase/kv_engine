add_library(mc_client_connection STATIC
            async_client_connection.cc
            async_client_connection.h
            client_connection.h
            client_connection.cc
            client_connection_map.cc
            client_connection_map.h
            client_mcbp_commands.cc
            client_mcbp_commands.h
            cluster_config_map_utils.h
        )
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
    OpenSSL::SSL
    platform
    ${COUCHBASE_NETWORK_LIBS}
  PRIVATE
    json_web_token
    folly_io_callbacks
    cbcompress
    json_validator
    xattr)
