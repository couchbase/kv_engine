cb_add_test_executable(mcauthjammer
                       mcauthjammer.cc)
kv_enable_pch(mcauthjammer)
target_link_libraries(mcauthjammer
                      PRIVATE
                      mc_client_connection mc_program_utils platform spdlog::spdlog)
add_sanitizers(mcauthjammer)
