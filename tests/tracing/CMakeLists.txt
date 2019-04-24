add_executable(tracing_test
               tracing_test.cc)

target_link_libraries(tracing_test
  mcd_tracing memcached_daemon gtest gtest_main)

add_sanitizers(tracing_test)

add_test(NAME tracing-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND tracing_test)

add_executable(tracing_bench tracing_bench.cc)

target_include_directories(tracing_bench SYSTEM PRIVATE
                           ${benchmark_SOURCE_DIR}/include)

target_link_libraries(tracing_bench PRIVATE benchmark mock_server)

add_sanitizers(tracing_bench)
