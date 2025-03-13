cb_add_test_executable(mktoken mktoken.cc)
kv_enable_pch(mktoken)
target_link_libraries(mktoken PRIVATE json_web_token platform)
add_sanitizers(mktoken)
