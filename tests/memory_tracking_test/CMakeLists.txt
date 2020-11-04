if (NOT MEMORY_ALLOCATOR STREQUAL "system")
  add_executable(memcached_memory_tracking_test
    $<TARGET_OBJECTS:memory_tracking>
    memory_tracking_test.cc)
  target_link_libraries(memcached_memory_tracking_test
  memcached_logger
  platform gtest
  ${MALLOC_LIBRARIES})
  add_dependencies(memcached_memory_tracking_test memcached_memory_tracking_plugin)

  add_test(NAME memcached-memory-tracking
           COMMAND memcached_memory_tracking_test $<TARGET_FILE:memcached_memory_tracking_plugin>)

  add_library(memcached_memory_tracking_plugin MODULE
              memory_tracking_plugin.cc)
  target_link_libraries(memcached_memory_tracking_plugin platform)
endif (NOT MEMORY_ALLOCATOR STREQUAL "system")
