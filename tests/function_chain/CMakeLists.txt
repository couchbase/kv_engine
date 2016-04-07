ADD_EXECUTABLE(memcached_function_chain_test
               function_chain_test.cc)
TARGET_LINK_LIBRARIES(memcached_function_chain_test gtest gtest_main)
ADD_TEST(NAME memcached-function-chain-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_function_chain_test)