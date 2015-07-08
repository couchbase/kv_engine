ADD_EXECUTABLE(memcached_sizes
               sizes.cc
               ${Memcached_SOURCE_DIR}/utilities/protocol2text.cc)
ADD_TEST(NAME memcached-sizes
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_sizes)
