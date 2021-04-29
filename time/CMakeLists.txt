ADD_LIBRARY(mcd_time STATIC
            isotime.cc
            ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
set_property(TARGET mcd_time PROPERTY POSITION_INDEPENDENT_CODE 1)
TARGET_LINK_LIBRARIES(mcd_time platform)
kv_enable_pch(mcd_time)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    cb_add_test_executable(memcached_timestamp_test timestamp_test.cc
                   ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
    kv_enable_pch(memcached_timestamp_test)
    TARGET_LINK_LIBRARIES(memcached_timestamp_test mcd_time)
    ADD_TEST(NAME memcached-timestamp-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached_timestamp_test)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
