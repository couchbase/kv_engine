add_executable(rangescan rangescan.cc)
kv_enable_pch(rangescan)
target_link_libraries(rangescan PRIVATE mc_program_utils mc_client_connection mcbp)
install(TARGETS rangescan RUNTIME DESTINATION bin/priv)
