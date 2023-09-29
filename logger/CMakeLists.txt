add_library(memcached_logger STATIC
            logger.h
            logger_config.cc
            logger_config.h
            spdlogger.cc
            custom_rotating_file_sink.cc
            custom_rotating_file_sink.h
            logger_test_fixture.h)
set_property(TARGET memcached_logger PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(memcached_logger)
cb_enable_unity_build(memcached_logger)

target_link_libraries(memcached_logger PUBLIC platform spdlog::spdlog)
add_sanitizers(memcached_logger)

cb_add_test_executable(memcached_logger_test
               logger_config_test.cc
               logger_test.cc
               logger_test_fixture.cc)
cb_enable_unity_build(memcached_logger_test)
kv_enable_pch(memcached_logger_test)
target_link_libraries(memcached_logger_test PRIVATE GTest::gtest GTest::gtest_main mcd_util memcached_logger platform)
target_include_directories(memcached_logger_test INTERFACE PRIVATE ${Platform_SOURCE_DIR}/external)
add_sanitizers(memcached_logger_test)
add_test(NAME memcached-spdlogger-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_logger_test)

cb_add_test_executable(memcached_logger_benchmark logger_benchmark.cc)
kv_enable_pch(memcached_logger_benchmark)
target_link_libraries(memcached_logger_benchmark PRIVATE benchmark::benchmark memcached_logger)
