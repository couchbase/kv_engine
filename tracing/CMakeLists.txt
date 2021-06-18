add_library(mcd_tracing STATIC tracer.cc)
set_property(TARGET mcd_tracing PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mcd_tracing PRIVATE engine_utilities platform)
add_sanitizers(mcd_tracing)
kv_enable_pch(mcd_tracing)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    cb_add_test_executable(tracing_test tracing_test.cc)
    kv_enable_pch(tracing_test)
    target_link_libraries(tracing_test
            PRIVATE
                    gtest gtest_main
                    mcd_tracing
                    memcached_daemon)
    add_sanitizers(tracing_test)
    add_test(NAME tracing-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND tracing_test)

endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
