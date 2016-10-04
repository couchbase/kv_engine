IF (NOT MEMORY_ALLOCATOR STREQUAL "system")
  # Need to ensure that the directory containing
  # memcached_memory_tracking_test is added to the RPATH so we can
  # successfully dlopen() the plugin.
  LINK_DIRECTORIES(${Memcached_BINARY_DIR})

  ADD_EXECUTABLE(memcached_memory_tracking_test
    $<TARGET_OBJECTS:memory_tracking>
    ${Memcached_SOURCE_DIR}/utilities/extension_loggers.c
    memory_tracking_test.cc)
  TARGET_LINK_LIBRARIES(memcached_memory_tracking_test platform ${MALLOC_LIBRARIES})
  ADD_TEST(NAME memcached-memory-tracking COMMAND memcached_memory_tracking_test)

  ADD_LIBRARY(memcached_memory_tracking_plugin MODULE
              memory_tracking_plugin.cc)
  TARGET_LINK_LIBRARIES(memcached_memory_tracking_plugin platform)
  SET_TARGET_PROPERTIES(memcached_memory_tracking_plugin PROPERTIES PREFIX "")
  IF(APPLE)
    SET_TARGET_PROPERTIES(memcached_memory_tracking_plugin PROPERTIES
      SUFFIX ".dylib")
  ENDIF(APPLE)
ENDIF (NOT MEMORY_ALLOCATOR STREQUAL "system")
