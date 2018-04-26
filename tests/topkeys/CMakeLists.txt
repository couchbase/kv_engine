add_executable(memcached_topkeys_test topkeys_test.cc)
target_link_libraries(memcached_topkeys_test memcached_daemon gtest gtest_main)
add_sanitizers(memcached_topkeys_test)

add_test(NAME memcached_topkeys_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_topkeys_test)

if (NOT WIN32)
  add_executable(memcached_topkeys_benchmark topkeys_benchmark.cc)
  target_link_libraries(memcached_topkeys_benchmark memcached_daemon benchmark)
endif (NOT WIN32)
