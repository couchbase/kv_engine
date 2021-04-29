# Most of the source is used both in the unit test and the actual program
# Build a static archive of the source to avoid compiling it twice
add_library(audit_generator_utilities OBJECT
            generator_event.cc
            generator_event.h
            generator_module.cc
            generator_module.h
            generator_utilities.cc
            generator_utilities.h)
kv_enable_pch(audit_generator_utilities)

add_executable(auditevent_generator
               auditevent_generator.cc
               auditevent_generator.h
               $<TARGET_OBJECTS:audit_generator_utilities>)
kv_enable_pch(auditevent_generator)
add_sanitizers(auditevent_generator)

target_link_libraries(auditevent_generator platform)

# When we're running the commit validation for memcached we don't
# download all of the external modules so we'll just use a
# subset of the audit descriptors.
if (COUCHBASE_KV_COMMIT_VALIDATION)
    SET(MODULE_DESCRIPTION ${Memcached_SOURCE_DIR}/auditd/etc/kv_commit_validation_module_descriptors.json)
else ()
    SET(MODULE_DESCRIPTION ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json)
endif (COUCHBASE_KV_COMMIT_VALIDATION)


add_custom_command(OUTPUT
                     ${Memcached_BINARY_DIR}/auditd/audit_events.json
                     ${Memcached_BINARY_DIR}/auditd/auditd_audit_events.h
                     ${Memcached_BINARY_DIR}/memcached_audit_events.h
                  COMMAND
                     auditevent_generator
                            -r ${CMAKE_SOURCE_DIR}
                            -b ${CMAKE_BINARY_DIR}
                            -o ${Memcached_BINARY_DIR}/auditd/audit_events.json
                            -i ${MODULE_DESCRIPTION}
                  DEPENDS
                        auditevent_generator
                        ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json
                        ${Memcached_SOURCE_DIR}/auditd/etc/auditd_descriptor.json
                        ${Memcached_SOURCE_DIR}/etc/memcached_descriptor.json
                  COMMENT "Generating audit descriptor definitions"
                  VERBATIM)

add_custom_target(generate_audit_descriptors ALL
                  DEPENDS ${Memcached_BINARY_DIR}/auditd/audit_events.json)

install(FILES
        ${Memcached_BINARY_DIR}/auditd/audit_events.json
        DESTINATION etc/security)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    cb_add_test_executable(audit_generator_unit_test
                   generator_event_test.cc
                   generator_module_test.cc
                   $<TARGET_OBJECTS:audit_generator_utilities>)
    kv_enable_pch(audit_generator_unit_test)
    add_sanitizers(audit_generator_unit_test)
    target_link_libraries(audit_generator_unit_test platform gtest gtest_main
                          mcd_util)
    add_test(NAME audit_generator_unit_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND audit_generator_unit_test)
endif()
