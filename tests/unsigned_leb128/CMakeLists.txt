add_executable(memcached_unsigned_leb128_test
               unsigned_leb128_test.cc)
add_executable(memcached_unsigned_leb128_bench
               unsigned_leb128_bench.cc)
target_link_libraries(memcached_unsigned_leb128_test gtest gtest_main)
target_link_libraries(memcached_unsigned_leb128_bench benchmark gtest gtest_main)
target_include_directories(memcached_unsigned_leb128_bench
                           SYSTEM PRIVATE ${benchmark_SOURCE_DIR}/include)

add_test(NAME memcached_unsigned_leb128_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_unsigned_leb128_test)