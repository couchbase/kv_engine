add_executable(dbfill dbfill.cc
        ${PROJECT_SOURCE_DIR}/engines/ep/src/crc32.c
        $<TARGET_OBJECTS:mc_program_utils>
        )
target_link_libraries(dbfill PRIVATE mc_client_connection platform)
add_sanitizers(dbfill)
