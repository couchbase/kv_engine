add_library(couchfile_upgrade_objects OBJECT couchfile.cc
                                  couchstore_helpers.cc
                                  input_couchfile.cc
                                  output_couchfile.cc)
target_link_libraries(couchfile_upgrade_objects PUBLIC couchstore)

add_executable(couchfile_upgrade couchfile_upgrade.cc
                                 $<TARGET_OBJECTS:couchfile_upgrade_objects>)
target_link_libraries(couchfile_upgrade couchstore mcd_util platform
        ep-engine_collections)
add_sanitizers(couchfile_upgrade)

install(TARGETS couchfile_upgrade RUNTIME DESTINATION bin)
