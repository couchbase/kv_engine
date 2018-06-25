ADD_EXECUTABLE(json-test-bench
        json_test_bench.cc)
TARGET_LINK_LIBRARIES(json-test-bench
                      benchmark cJSON dirutils gtest platform)