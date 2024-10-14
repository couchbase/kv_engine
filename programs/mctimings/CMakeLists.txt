add_executable(mctimings mctimings.cc)
kv_enable_pch(mctimings)
target_link_libraries(mctimings
        PRIVATE
                fmt::fmt
                mc_client_connection
                mc_program_utils
                platform)
add_sanitizers(mctimings)
install(TARGETS mctimings RUNTIME DESTINATION bin)
AddToStandalonePackage(TARGETS mctimings PACKAGES admin_tools)
add_test(NAME mctimings_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mctimings --help)
