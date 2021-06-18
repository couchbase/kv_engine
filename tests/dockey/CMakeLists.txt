cb_add_test_executable(memcached_dockey_test
               dockey_test.cc)
kv_enable_pch(memcached_dockey_test)
target_link_libraries(memcached_dockey_test
        PRIVATE Folly::headers mcbp gtest gtest_main)
add_test(NAME memcached_dockey_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_dockey_test)
