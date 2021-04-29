cb_add_test_executable(histo-test-bench histogram_test_bench.cc)
kv_enable_pch(histo-test-bench)
target_include_directories(histo-test-bench
        SYSTEM PRIVATE ${benchmark_SOURCE_DIR}/include)
target_link_libraries(histo-test-bench benchmark
        hdr_histogram_static
        mcd_util memcached_daemon gtest)
add_sanitizers(histo-test-bench)
