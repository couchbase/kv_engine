ADD_LIBRARY(mcd_time STATIC
            isotime.cc
            ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
set_property(TARGET mcd_time PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mcd_time PRIVATE platform)
kv_enable_pch(mcd_time)

cb_add_test_executable(memcached_timestamp_test timestamp_test.cc
               ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
kv_enable_pch(memcached_timestamp_test)
target_link_libraries(memcached_timestamp_test PRIVATE mcd_time platform GTest::gtest GTest::gtest_main)
ADD_TEST(NAME memcached-timestamp-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_timestamp_test)
