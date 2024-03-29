ADD_LIBRARY(xattr STATIC
            ${PROJECT_SOURCE_DIR}/include/xattr/blob.h
            ${PROJECT_SOURCE_DIR}/include/xattr/key_validator.h
            ${PROJECT_SOURCE_DIR}/include/xattr/utils.h
            blob.cc
            key_validator.cc
            utils.cc)
SET_TARGET_PROPERTIES(xattr PROPERTIES POSITION_INDEPENDENT_CODE 1)
target_link_libraries(xattr PRIVATE cbcompress platform json_validator)
add_sanitizers(xattr)
kv_enable_pch(xattr)
cb_enable_unity_build(xattr)

cb_add_test_executable(xattr_test
        blob_test.cc
        blob_validator_test.cc
        key_validator_test.cc)
target_link_libraries(xattr_test
        PRIVATE
        xattr
        cbcompress
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main)
add_sanitizers(xattr_test)
cb_enable_unity_build(xattr_test)
kv_enable_pch(xattr_test)

add_test(NAME xattr_test COMMAND xattr_test)
