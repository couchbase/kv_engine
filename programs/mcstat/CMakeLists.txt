add_executable(mcstat mcstat.cc)
kv_enable_pch(mcstat)
target_link_libraries(mcstat PRIVATE mc_client_connection mc_program_utils platform)
add_sanitizers(mcstat)
install(TARGETS mcstat RUNTIME DESTINATION bin)
AddToStandalonePackage(TARGETS mcstat PACKAGES admin_tools)
add_test(NAME mcstat_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcstat --help)
add_test(NAME mcstat_statkey_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcstat --help=statkey)
add_test(NAME mcstat_statkey_thread_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcstat --help=threads)
