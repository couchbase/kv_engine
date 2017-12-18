ADD_EXECUTABLE(tracing_test
               tracing_test.cc)

TARGET_LINK_LIBRARIES(tracing_test mcd_tracing gtest gtest_main)

ADD_TEST(NAME tracing-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND tracing_test)
