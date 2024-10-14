add_executable(mcctl mcctl.cc)
kv_enable_pch(mcctl)
target_link_libraries(mcctl PRIVATE mc_client_connection mcd_util mc_program_utils platform)
add_sanitizers(mcctl)
install(TARGETS mcctl RUNTIME DESTINATION bin)
add_test(NAME mcctl_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcctl --help)
