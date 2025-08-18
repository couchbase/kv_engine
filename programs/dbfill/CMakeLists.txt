cb_add_test_executable(dbfill dbfill.cc
        ${PROJECT_SOURCE_DIR}/engines/ep/src/crc32.c)
target_link_libraries(dbfill PRIVATE json_generator mc_client_connection mc_program_utils platform)
add_sanitizers(dbfill)
