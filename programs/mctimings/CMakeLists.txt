add_executable(mctimings mctimings.cc $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mctimings)
target_link_libraries(mctimings
        PRIVATE
                fmt::fmt
                mc_client_connection
                platform)
add_sanitizers(mctimings)
install(TARGETS mctimings RUNTIME DESTINATION bin)
AddToStandalonePackage(TARGETS mctimings PACKAGES admin_tools)
add_test(NAME mctimings_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mctimings --help)
