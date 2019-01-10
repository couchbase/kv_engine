add_executable(histo-test-bench histogram_test_bench.cc)
target_include_directories(histo-test-bench
        PRIVATE ${benchmark_SOURCE_DIR}/include)
target_link_libraries(histo-test-bench benchmark
        hdr_histogram_static
        mcd_util memcached_daemon gtest)