cb_add_test_executable(memcached_errormap_sanity_check
               error_map_sanity_check.cc)
kv_enable_pch(memcached_errormap_sanity_check)

target_link_libraries(memcached_errormap_sanity_check
        PRIVATE
                Folly::headers
                memcached_daemon)
add_sanitizers(memcached_errormap_sanity_check)

ADD_TEST(NAME memcached_errormap_sanity_check
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_errormap_sanity_check ${Memcached_SOURCE_DIR}/etc/couchbase/kv/error_maps)
