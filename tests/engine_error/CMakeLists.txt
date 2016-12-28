INCLUDE_DIRECTORIES(AFTER ${gtest_SOURCE_DIR}/include)
ADD_EXECUTABLE(memcached-engine-error-test engine_error_test.cc)
TARGET_LINK_LIBRARIES(memcached-engine-error-test engine_utilities gtest gtest_main)
ADD_TEST(NAME memcached-engine-error-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-engine-error-test)
