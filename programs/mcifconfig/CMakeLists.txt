add_executable(mcifconfig mcifconfig.cc $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mcifconfig)
target_link_libraries(mcifconfig PRIVATE mc_client_connection mcd_util platform)
add_sanitizers(mcifconfig)
install(TARGETS mcifconfig RUNTIME DESTINATION bin)
add_test(NAME mcifconfig_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcifconfig --help)
