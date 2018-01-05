INCLUDE_DIRECTORIES(AFTER SYSTEM ${gtest_SOURCE_DIR}/include)
ADD_EXECUTABLE(memcached-doc-server-api-test
               doc_pre_expiry_test.cc
               ${PROJECT_SOURCE_DIR}/daemon/doc_pre_expiry.cc)
TARGET_LINK_LIBRARIES(memcached-doc-server-api-test xattr gtest gtest_main)
ADD_TEST(NAME memcached-doc-server-api-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-doc-server-api-test)
