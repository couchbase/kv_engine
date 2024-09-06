add_library(json_web_token STATIC builder.cc builder.h token.cc token.h)
set_property(TARGET json_web_token PROPERTY POSITION_INDEPENDENT_CODE 1)
platform_enable_pch(json_web_token)
target_link_libraries(json_web_token
        PUBLIC
        nlohmann_json::nlohmann_json
        PRIVATE
        fmt::fmt
        cbcrypto
        platform)

cb_add_test_executable(json_web_token_test jwt_test.cc)
platform_enable_pch(json_web_token_test)
target_link_libraries(json_web_token_test
        PRIVATE
        json_web_token
        platform
        GTest::gtest
        GTest::gtest_main)
add_test(NAME json_web_token_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND json_web_token_test)
