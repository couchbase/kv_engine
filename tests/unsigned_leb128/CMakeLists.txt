cb_add_test_executable(memcached_unsigned_leb128_test
               unsigned_leb128_test.cc)
target_link_libraries(memcached_unsigned_leb128_test PRIVATE
        platform_headers GTest::gtest GTest::gtest_main)
cb_add_test_executable(memcached_unsigned_leb128_bench
               unsigned_leb128_bench.cc)
target_link_libraries(memcached_unsigned_leb128_bench PRIVATE
                      platform_headers
                      benchmark::benchmark benchmark::benchmark_main)

add_test(NAME memcached_unsigned_leb128_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_unsigned_leb128_test)
