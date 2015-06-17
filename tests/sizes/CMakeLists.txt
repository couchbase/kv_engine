ADD_EXECUTABLE(memcached_sizes sizes.cc)
ADD_TEST(NAME memcached-sizes
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_sizes)
