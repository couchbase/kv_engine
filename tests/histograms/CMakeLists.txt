cb_add_test_executable(histo-test-bench histogram_test_bench.cc)
kv_enable_pch(histo-test-bench)
target_link_libraries(histo-test-bench PRIVATE
                      benchmark::benchmark benchmark::benchmark_main
                      platform mcd_util memcached_daemon
                      hdrhistogram
                      GTest::gtest)
add_sanitizers(histo-test-bench)
