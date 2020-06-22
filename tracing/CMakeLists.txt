add_library(mcd_tracing STATIC tracer.cc)
set_property(TARGET mcd_tracing PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mcd_tracing engine_utilities platform ${FOLLY_LIBRARIES})
add_sanitizers(mcd_tracing)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(tracing_test tracing_test.cc)
    target_link_libraries(tracing_test
                          mcd_tracing memcached_daemon gtest gtest_main)
    add_sanitizers(tracing_test)
    add_test(NAME tracing-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND tracing_test)

endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
