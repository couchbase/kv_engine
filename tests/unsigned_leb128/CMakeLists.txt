add_executable(memcached_unsigned_leb128_test
               unsigned_leb128_test.cc)
add_executable(memcached_unsigned_leb128_bench
               unsigned_leb128_bench.cc)
target_link_libraries(memcached_unsigned_leb128_test gtest gtest_main platform)
target_link_libraries(memcached_unsigned_leb128_bench benchmark gtest gtest_main)

add_test(NAME memcached_unsigned_leb128_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_unsigned_leb128_test)
