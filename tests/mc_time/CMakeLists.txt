ADD_EXECUTABLE(memcached_mc_time_test mc_time_test.cc
               ${Memcached_SOURCE_DIR}/daemon/mc_time.cc
               ${Memcached_SOURCE_DIR}/daemon/mc_time.h)

TARGET_LINK_LIBRARIES(memcached_mc_time_test
                      memcached_daemon
                      platform
                      gtest
                      gtest_main
                      ${LIBEVENT_LIBRARIES})
add_sanitizers(memcached_mc_time_test)

ADD_TEST(NAME memcached_mc_time_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_mc_time_test)
