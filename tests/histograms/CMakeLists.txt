cb_add_test_executable(histo-test-bench histogram_test_bench.cc)
kv_enable_pch(histo-test-bench)
target_link_libraries(histo-test-bench PRIVATE
                      benchmark::benchmark benchmark::benchmark_main
                      platform mcd_util memcached_daemon
                      hdrhistogram)
add_sanitizers(histo-test-bench)
if (NOT CB_SANITIZERS)
    add_test(NAME histo-test-bench
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND histo-test-bench --benchmark_min_time=0.0000001s)
endif()
