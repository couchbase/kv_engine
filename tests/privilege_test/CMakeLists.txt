ADD_EXECUTABLE(memcached_privilege_test
               ${Memcached_SOURCE_DIR}/daemon/privilege_database.cc
               ${Memcached_SOURCE_DIR}/daemon/privilege_database.h
               ${Memcached_SOURCE_DIR}/daemon/privileges.cc
               privilege_test.cc)

TARGET_LINK_LIBRARIES(memcached_privilege_test cJSON gtest gtest_main)
ADD_TEST(NAME memcached-privilege-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_privilege_test)
