IF (NOT MEMORY_ALLOCATOR STREQUAL "system")
  ADD_EXECUTABLE(memcached_memory_tracking_test
    $<TARGET_OBJECTS:memory_tracking>
    ${Memcached_SOURCE_DIR}/utilities/extension_loggers.c
    memory_tracking_test.cc)
  TARGET_LINK_LIBRARIES(memcached_memory_tracking_test platform gtest ${MALLOC_LIBRARIES})
  ADD_TEST(NAME memcached-memory-tracking COMMAND memcached_memory_tracking_test)
  APPEND_MALLOC_LINKER_FLAGS(memcached_memory_tracking_test)
ENDIF (NOT MEMORY_ALLOCATOR STREQUAL "system")
