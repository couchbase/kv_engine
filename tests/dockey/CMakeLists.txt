add_executable(memcached_dockey_test
               dockey_test.cc)
target_link_libraries(memcached_dockey_test mcbp gtest gtest_main)
add_test(NAME memcached_dockey_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_dockey_test)