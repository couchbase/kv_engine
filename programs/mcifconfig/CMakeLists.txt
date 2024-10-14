add_executable(mcifconfig mcifconfig.cc)
kv_enable_pch(mcifconfig)
target_link_libraries(mcifconfig PRIVATE mc_client_connection mcd_util mc_program_utils platform)
add_sanitizers(mcifconfig)
install(TARGETS mcifconfig RUNTIME DESTINATION bin)
add_test(NAME mcifconfig_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcifconfig --help)
