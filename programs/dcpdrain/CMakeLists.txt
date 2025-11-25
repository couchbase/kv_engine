add_executable(dcpdrain dcpdrain.cc)
kv_enable_pch(dcpdrain)
target_link_libraries(dcpdrain PRIVATE mc_program_utils mc_client_connection mcbp)
install(TARGETS dcpdrain RUNTIME DESTINATION bin/priv)
