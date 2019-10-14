if (NOT MEMORY_ALLOCATOR STREQUAL "system")
  # Need to ensure that the directory containing
  # memcached_memory_tracking_test is added to the RPATH so we can
  # successfully dlopen() the plugin.
  link_directories(${Memcached_BINARY_DIR})

  add_executable(memcached_memory_tracking_test
    $<TARGET_OBJECTS:memory_tracking>
    memory_tracking_test.cc)
  target_link_libraries(memcached_memory_tracking_test
  memcached_logger
  platform gtest
  ${MALLOC_LIBRARIES})

  add_test(NAME memcached-memory-tracking
           WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
           COMMAND memcached_memory_tracking_test)

  add_library(memcached_memory_tracking_plugin MODULE
              memory_tracking_plugin.cc)
  target_link_libraries(memcached_memory_tracking_plugin platform)
  set_target_properties(memcached_memory_tracking_plugin PROPERTIES PREFIX "")
endif (NOT MEMORY_ALLOCATOR STREQUAL "system")
