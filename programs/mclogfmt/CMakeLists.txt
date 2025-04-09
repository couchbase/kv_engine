add_library(mclogfmt_static OBJECT mclogfmt.cc)
target_link_libraries(mclogfmt_static PUBLIC platform mcd_util)

cb_add_test_executable(mclogfmt mclogfmt_main.cc)
kv_enable_pch(mclogfmt)
cb_enable_unity_build(mclogfmt)
target_link_libraries(mclogfmt PRIVATE mclogfmt_static)
add_sanitizers(mclogfmt)

cb_add_test_executable(mclogfmt_test mclogfmt_test.cc)
target_link_libraries(mclogfmt_test PRIVATE
                      mclogfmt_static
                      GTest::gtest
                      GTest::gtest_main)
add_sanitizers(mclogfmt_test)
kv_enable_pch(mclogfmt_test)

add_test(NAME mclogfmt_test COMMAND mclogfmt_test)
