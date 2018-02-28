ADD_EXECUTABLE(memcached_sizes
               sizes.cc)
TARGET_LINK_LIBRARIES(memcached_sizes mcd_util memcached_logger)
IF(ENABLE_DTRACE)
    ADD_DEPENDENCIES(memcached_sizes generate_memcached_dtrace_h)
ENDIF(ENABLE_DTRACE)

ADD_TEST(NAME memcached-sizes
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_sizes)
