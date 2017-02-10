ADD_EXECUTABLE(memcached_privilege_test
               privilege_test.cc)

TARGET_LINK_LIBRARIES(memcached_privilege_test cJSON memcached_rbac
                      gtest gtest_main)
ADD_TEST(NAME memcached-privilege-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_privilege_test)
