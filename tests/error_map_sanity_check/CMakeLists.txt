ADD_EXECUTABLE(memcached_errormap_sanity_check
               error_map_sanity_check.cc)

TARGET_LINK_LIBRARIES(memcached_errormap_sanity_check memcached_daemon)

ADD_TEST(NAME memcached_errormap_sanity_check
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_errormap_sanity_check ${Memcached_SOURCE_DIR}/etc/error_maps)
