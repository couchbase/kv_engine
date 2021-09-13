add_library(memcached_logger STATIC
            logger.h
            logger_config.cc
            logger_config.h
            spdlogger.cc
            custom_rotating_file_sink.cc
            custom_rotating_file_sink.h
            logger_test_fixture.h)
kv_enable_pch(memcached_logger)

target_link_libraries(memcached_logger PUBLIC platform spdlog)
add_sanitizers(memcached_logger)

cb_add_test_executable(memcached_logger_test
               logger_test.cc
               logger_test_fixture.cc)
kv_enable_pch(memcached_logger_test)
target_link_libraries(memcached_logger_test PRIVATE GTest::gtest GTest::gtest_main mcd_util memcached_logger platform)
target_include_directories(memcached_logger_test INTERFACE PRIVATE ${Platform_SOURCE_DIR}/external)
add_sanitizers(memcached_logger_test)
add_test(NAME memcached-spdlogger-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_logger_test)

cb_add_test_executable(memcached_logger_benchmark logger_benchmark.cc)
kv_enable_pch(memcached_logger_benchmark)
target_include_directories(memcached_logger_benchmark
                           SYSTEM PRIVATE
                           ${benchmark_SOURCE_DIR}/include)
target_link_libraries(memcached_logger_benchmark PRIVATE benchmark memcached_logger)
