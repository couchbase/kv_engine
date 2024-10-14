add_executable(mctrace mctrace.cc)
kv_enable_pch(mctrace)
target_link_libraries(mctrace PRIVATE
                      mc_client_connection
                      mc_program_utils
                      mcd_util
                      platform)
add_sanitizers(mctrace)
install(TARGETS mctrace RUNTIME DESTINATION bin)
add_test(NAME mctrace_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mctrace --help)
