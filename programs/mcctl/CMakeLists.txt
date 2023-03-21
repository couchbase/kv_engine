add_executable(mcctl mcctl.cc $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mcctl)
target_link_libraries(mcctl PRIVATE mc_client_connection mcd_util platform)
add_sanitizers(mcctl)
install(TARGETS mcctl RUNTIME DESTINATION bin)
add_test(NAME mcctl_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcctl --help)
