cb_add_test_executable(mclogfmt mclogfmt.cc)
kv_enable_pch(mclogfmt)
target_link_libraries(mclogfmt PRIVATE platform mcd_util)
add_sanitizers(mclogfmt)
