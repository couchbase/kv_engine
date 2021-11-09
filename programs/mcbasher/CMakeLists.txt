cb_add_test_executable(mcbasher mcbasher.cc $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mcbasher)
target_link_libraries(mcbasher PRIVATE mc_client_connection mcd_util platform)
add_sanitizers(mcbasher)
