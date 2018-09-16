ADD_EXECUTABLE(json-test-bench
        json_test_bench.cc)
TARGET_INCLUDE_DIRECTORIES(json-test-bench
    PRIVATE
    ${benchmark_SOURCE_DIR}/include)
TARGET_LINK_LIBRARIES(json-test-bench
                      benchmark cJSON dirutils gtest platform)