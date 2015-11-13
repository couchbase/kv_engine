ADD_EXECUTABLE(memcached_topkeys_bench
               ${PROJECT_SOURCE_DIR}/daemon/topkeys.cc
               topkeys_bench.cc)
TARGET_LINK_LIBRARIES(memcached_topkeys_bench cJSON gtest gtest_main platform)
ADD_TEST(NAME memcached_topkeys_bench
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_topkeys_bench)
