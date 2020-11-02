IF (NOT MEMORY_ALLOCATOR STREQUAL "system")
  ADD_EXECUTABLE(memcached_memory_tracking_test
    $<TARGET_OBJECTS:memory_tracking>
    memory_tracking_test.cc)
  TARGET_LINK_LIBRARIES(memcached_memory_tracking_test memcached_logger platform gtest ${MALLOC_LIBRARIES})
  ADD_TEST(NAME memcached-memory-tracking
    COMMAND memcached_memory_tracking_test $<TARGET_FILE:memcached_memory_tracking_plugin>)
  ADD_DEPENDENCIES(memcached_memory_tracking_test memcached_memory_tracking_plugin)

  ADD_LIBRARY(memcached_memory_tracking_plugin MODULE
              memory_tracking_plugin.cc)
  TARGET_LINK_LIBRARIES(memcached_memory_tracking_plugin platform)
ENDIF (NOT MEMORY_ALLOCATOR STREQUAL "system")
