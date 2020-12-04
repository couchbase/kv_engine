add_executable(memcached_topkeys_test topkeys_test.cc)
kv_enable_pch(memcached_topkeys_test)
target_link_libraries(memcached_topkeys_test memcached_daemon gtest gtest_main)
add_sanitizers(memcached_topkeys_test)

add_test(NAME memcached_topkeys_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_topkeys_test)

if (NOT WIN32)
  add_executable(memcached_topkeys_benchmark topkeys_benchmark.cc)
  kv_enable_pch(memcached_topkeys_benchmark)
  target_include_directories(memcached_topkeys_benchmark
      SYSTEM PRIVATE
      ${benchmark_SOURCE_DIR}/include)
  target_link_libraries(memcached_topkeys_benchmark memcached_daemon benchmark)
  add_sanitizers(memcached_topkeys_benchmark)
endif (NOT WIN32)
