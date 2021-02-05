add_executable(mctestauth mctestauth.cc $<TARGET_OBJECTS:mc_program_utils>)
target_link_libraries(mctestauth mc_client_connection platform)
add_sanitizers(mctestauth)
install(TARGETS mctestauth RUNTIME DESTINATION bin)
