cb_add_test_executable(memcached-basic-engine-tests
               basic_engine_testsuite.cc
               engine_testsuite.cc)
kv_enable_pch(memcached-basic-engine-tests)
add_sanitizers(memcached-basic-engine-tests)
target_link_libraries(memcached-basic-engine-tests PRIVATE
                      mock_server
                      memcached_daemon
                      GTest::gtest
                      GTest::gtest_main)

add_test(NAME memcached-basic-engine-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-basic-engine-tests)
