add_executable(memcached-basic-engine-tests
               basic_engine_testsuite.cc
               engine_testsuite.cc)
add_sanitizers(memcached-basic-engine-tests)
target_link_libraries(memcached-basic-engine-tests
                      mock_server
                      memcached_daemon
                      gtest
                      gtest_main)

add_test(NAME memcached-basic-engine-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-basic-engine-tests)
