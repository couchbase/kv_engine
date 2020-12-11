if (NOT MEMORY_ALLOCATOR STREQUAL "system")
    add_executable(memcached_memory_tracking_test
                   memory_tracking_test.cc)
    target_link_libraries(memcached_memory_tracking_test
                          platform
                          platform_cb_malloc_arena
                          gtest
                          ${MALLOC_LIBRARIES})

    add_test(NAME memcached-memory-tracking
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached_memory_tracking_test)
endif (NOT MEMORY_ALLOCATOR STREQUAL "system")
