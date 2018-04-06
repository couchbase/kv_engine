ADD_EXECUTABLE(memcached_datatype_test datatype_test.cc)
TARGET_LINK_LIBRARIES(memcached_datatype_test gtest gtest_main memcached_daemon platform)
add_sanitizers(memcached_datatype_test)

ADD_TEST(NAME memcached_datatype-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_datatype_test)