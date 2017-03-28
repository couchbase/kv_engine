ADD_EXECUTABLE(memcached_datatype_test
               ${Memcached_SOURCE_DIR}/daemon/datatype.cc
               ${Memcached_SOURCE_DIR}/daemon/datatype.h
               ${Memcached_SOURCE_DIR}/daemon/settings.cc
               ${Memcached_SOURCE_DIR}/daemon/settings.h
               datatype_test.cc)
TARGET_LINK_LIBRARIES(memcached_datatype_test gtest gtest_main memcached_daemon platform)
ADD_TEST(NAME memcached_datatype-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_datatype_test)