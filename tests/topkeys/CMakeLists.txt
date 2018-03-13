ADD_EXECUTABLE(memcached_topkeys_bench topkeys_bench.cc)
TARGET_LINK_LIBRARIES(memcached_topkeys_bench memcached_daemon gtest gtest_main)
add_sanitizers(memcached_topkeys_bench)

ADD_TEST(NAME memcached_topkeys_bench
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_topkeys_bench)
