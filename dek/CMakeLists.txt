add_library(mcd_dek STATIC
            manager.cc
            manager.h
        )
set_property(TARGET mcd_dek PROPERTY POSITION_INDEPENDENT_CODE 1)
cb_enable_unity_build(mcd_dek)

target_link_libraries(mcd_dek
        PUBLIC
        platform
        PRIVATE
        cbcrypto
        json_validator
        mcd_util
        nlohmann_json::nlohmann_json)
add_sanitizers(mcd_dek)
kv_enable_pch(mcd_dek)

cb_add_test_executable(dek_unit_test manager_unit_test.cc)
cb_enable_unity_build(dek_unit_test)
kv_enable_pch(dek_unit_test)
target_link_libraries(dek_unit_test
        PRIVATE
        mcd_dek
        GTest::gtest
        GTest::gtest_main)
add_test(NAME dek_unit_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND dek_unit_test)
add_sanitizers(dek_unit_test)
