add_library(json_validator STATIC
            syntax_validator.cc
            syntax_validator.h)
set_target_properties(json_validator PROPERTIES POSITION_INDEPENDENT_CODE 1)
target_link_libraries(json_validator PRIVATE
                      JSON_checker nlohmann_json::nlohmann_json)
target_include_directories(json_validator PRIVATE ${Platform_SOURCE_DIR}/include)

cb_add_test_executable(json_validator_test syntax_validator_test.cc)
target_link_libraries(json_validator_test PRIVATE
        json_validator Folly::headers GTest::gtest GTest::gtest_main)
add_test(NAME json_validator_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND json_validator_test)

cb_add_test_executable(json_validator_bench syntax_validator_bench.cc)
target_link_libraries(json_validator_bench PRIVATE
        Folly::headers benchmark::benchmark GTest::gtest json_validator)
