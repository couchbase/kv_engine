add_executable(mctrace mctrace.cc $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mctrace)
target_link_libraries(mctrace PRIVATE
                      mc_client_connection
                      mcd_util
                      platform)
add_sanitizers(mctrace)
install(TARGETS mctrace RUNTIME DESTINATION bin)
add_test(NAME mctrace_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mctrace --help)
