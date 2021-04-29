cb_add_test_executable(mclogsplit mclogsplit.cc)
kv_enable_pch(mclogsplit)
target_link_libraries(mclogsplit platform mcd_util)
add_sanitizers(mclogsplit)
