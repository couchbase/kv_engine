add_library(folly_io_callbacks OBJECT
        read_callback.cc read_callback.h
        write_callback.cc write_callback.h)
cb_enable_unity_build(folly_io_callbacks)
target_link_libraries(folly_io_callbacks PUBLIC
        mcbp
        platform
        nlohmann_json::nlohmann_json
        fmt::fmt
        Folly::headers)
add_sanitizers(folly_io_callbacks)
kv_enable_pch(folly_io_callbacks)

cb_add_test_executable(folly_io_callback_tests
        read_callback_test.cc)
add_sanitizers(folly_io_callback_tests)
cb_enable_unity_build(folly_io_callback_tests)
kv_enable_pch(folly_io_callback_tests)
target_link_libraries(folly_io_callback_tests PRIVATE
        folly_io_callbacks
        GTest::gtest
        GTest::gtest_main)
add_test(NAME folly_io_callback_tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND folly_io_callback_tests)
