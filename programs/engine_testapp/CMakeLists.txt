add_library(mock_server STATIC
            mock_cookie.cc
            mock_cookie.h
            mock_engine.cc
            mock_engine.h
            mock_server.cc
            mock_server.h
)
set_property(TARGET mock_server PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(mock_server)
target_link_libraries(mock_server PUBLIC memcached_logger)
target_link_libraries(mock_server PRIVATE platform_headers)
target_link_libraries(mock_server INTERFACE
                      mcd_tracing
                      mcd_util
                      platform
                      hdrhistogram
                      json_validator
                      xattr
                      ${MALLOC_LIBRARIES})

add_library(engine_testapp OBJECT engine_testapp.cc
        ${Memcached_SOURCE_DIR}/daemon/bucket_type.cc
        ${Memcached_SOURCE_DIR}/daemon/enginemap.cc
        ${Memcached_SOURCE_DIR}/daemon/enginemap.h
        ${Memcached_SOURCE_DIR}/include/memcached/bucket_type.h
)
kv_enable_pch(engine_testapp)
target_link_libraries(engine_testapp PRIVATE phosphor)
target_link_libraries(engine_testapp PUBLIC Folly::headers hdrhistogram memcached_logger)
cb_enable_unity_build(engine_testapp)

# An object library will not propagate dependencies
# https://gitlab.kitware.com/cmake/cmake/issues/18090
# Here we create an interface library to bring the libraries that
# will be needed in a target
add_library(engine_testapp_dependencies INTERFACE)
target_link_libraries(engine_testapp_dependencies INTERFACE
                      phosphor
                      platform
                      hdrhistogram
                      mock_server
                      crash_engine
                      ep
                      ewouldblock_engine
                      nobucket
                      cbsocket
                      ${COUCHBASE_NETWORK_LIBS})
add_sanitizers(engine_testapp)
