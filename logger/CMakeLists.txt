add_library(memcached_logger STATIC
            logger.h
            logger_config.cc
            logger_config.h
            spdlogger.cc
            custom_rotating_file_sink.cc
            custom_rotating_file_sink.h
            logger_test_fixture.h)

target_link_libraries(memcached_logger PUBLIC platform spdlog)
add_sanitizers(memcached_logger)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(memcached_logger_test
                   logger_test.cc
                   logger_test_fixture.cc)
    target_link_libraries(memcached_logger_test gtest gtest_main mcd_util memcached_logger platform)
    target_include_directories(memcached_logger_test INTERFACE PRIVATE ${Platform_SOURCE_DIR}/external)
    add_sanitizers(memcached_logger_test)
    add_test(NAME memcached-spdlogger-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached_logger_test)

    add_executable(memcached_logger_benchmark logger_benchmark.cc)
    target_include_directories(memcached_logger_benchmark
                               SYSTEM PRIVATE
                               ${benchmark_SOURCE_DIR}/include)
    target_link_libraries(memcached_logger_benchmark benchmark memcached_logger)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
