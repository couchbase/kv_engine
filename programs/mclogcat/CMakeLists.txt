cb_add_test_executable(mclogcat mclogcat.cc)
kv_enable_pch(mclogcat)
target_link_libraries(mclogcat PRIVATE platform mcd_util)
add_sanitizers(mclogcat)
