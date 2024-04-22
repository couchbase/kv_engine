cb_add_test_executable(mcauthjammer
                       mcauthjammer.cc
                       $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mcauthjammer)
target_link_libraries(mcauthjammer
                      PRIVATE
                      mc_client_connection platform spdlog::spdlog)
add_sanitizers(mcauthjammer)
