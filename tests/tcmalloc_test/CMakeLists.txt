IF (("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "tcmalloc") OR
    ("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "jemalloc"))
  ADD_EXECUTABLE(memcached_tcmalloc_test ${MEMORY_TRACKING_SRCS}
    ${Memcached_SOURCE_DIR}/utilities/extension_loggers.c
    tcmalloc_test.cc)
  TARGET_LINK_LIBRARIES(memcached_tcmalloc_test platform ${MALLOC_LIBRARIES})
  ADD_TEST(memcached-tcmalloc-hooks memcached_tcmalloc_test)
  APPEND_MALLOC_LINKER_FLAGS(memcached_tcmalloc_test)
ENDIF (("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "tcmalloc") OR
       ("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "jemalloc"))
