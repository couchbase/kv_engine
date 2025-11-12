if (NOT DEFINED CB_BUILD_CONFIGURATION)
    if (COUCHBASE_KV_COMMIT_VALIDATION)
        set(CB_BUILD_CONFIGURATION "KV_CV")
    elseif (BUILD_ENTERPRISE)
        set(CB_BUILD_CONFIGURATION "EE")
    else ()
        set(CB_BUILD_CONFIGURATION "CE")
    endif ()
endif ()

# Most of the source is used both in the unit test and the actual program
# Build a static archive of the source to avoid compiling it twice
add_library(audit_generator_utilities OBJECT
            generator_event.cc
            generator_event.h
            generator_module.cc
            generator_module.h
            generator_utilities.cc
            generator_utilities.h)
target_compile_definitions(audit_generator_utilities PRIVATE -DCB_BUILD_CONFIGURATION="${CB_BUILD_CONFIGURATION}")
target_link_libraries(audit_generator_utilities
                      PRIVATE nlohmann_json::nlohmann_json platform fmt::fmt)
kv_enable_pch(audit_generator_utilities)
cb_enable_unity_build(audit_generator_utilities)

add_executable(auditevent_generator
               auditevent_generator.cc
               auditevent_generator.h)
kv_enable_pch(auditevent_generator)
add_sanitizers(auditevent_generator)

target_link_libraries(auditevent_generator PRIVATE
                      audit_generator_utilities platform)

# When we're running the commit validation for memcached we don't
# download all of the external modules so we'll just use a
# subset of the audit descriptors.
list(APPEND AUDIT_MODULE_DEPENDENCIES auditevent_generator
        ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json
        ${Memcached_SOURCE_DIR}/auditd/etc/auditd_descriptor.json
        ${Memcached_SOURCE_DIR}/etc/memcached_descriptor.json)

foreach(audit_descriptor_file
            ${CMAKE_SOURCE_DIR}/ns_server/etc/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/goproj/src/github.com/couchbase/goxdcr/etc/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/cbft/etc/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/goproj/src/github.com/couchbase/query/etc/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/couchdb/src/couch_audit/etc/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/goproj/src/github.com/couchbase/indexing/secondary/audit/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/goproj/src/github.com/couchbase/eventing/audit/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/analytics/cbas/cbas-server/src/main/resources/com/couchbase/analytics/audit/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/cbbs/audit/audit_descriptor.json
            ${CMAKE_SOURCE_DIR}/backup/etc/audit_descriptor.json
        )

    if (EXISTS ${audit_descriptor_file})
       list(APPEND AUDIT_MODULE_DEPENDENCIES ${audit_descriptor_file})
    endif()
endforeach()

add_custom_command(OUTPUT
                     ${Memcached_BINARY_DIR}/auditd/audit_events.json
                     ${Memcached_BINARY_DIR}/auditd/couchbase_audit_events.h
                     ${Memcached_BINARY_DIR}/auditd/audit_descriptor_manager_defs.cc
                  COMMAND
                     auditevent_generator
                            -r ${CMAKE_SOURCE_DIR}
                            -b ${CMAKE_BINARY_DIR}
                            -o ${Memcached_BINARY_DIR}/auditd/audit_events.json
                            -i ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json
                            -f ${Memcached_BINARY_DIR}/auditd/couchbase_audit_events.h
                  DEPENDS
                     ${AUDIT_MODULE_DEPENDENCIES}
                  COMMENT "Generating audit descriptor definitions for configuration ${CB_BUILD_CONFIGURATION}"
                  VERBATIM)

add_custom_target(generate_audit_descriptors ALL
                  DEPENDS ${Memcached_BINARY_DIR}/auditd/audit_events.json)

install(FILES
        ${Memcached_BINARY_DIR}/auditd/audit_events.json
        DESTINATION etc/security)

cb_add_test_executable(audit_generator_unit_test
               generator_event_test.cc
               generator_module_test.cc)
cb_enable_unity_build(audit_generator_unit_test)
kv_enable_pch(audit_generator_unit_test)
add_sanitizers(audit_generator_unit_test)
target_link_libraries(audit_generator_unit_test
        PRIVATE
                audit_generator_utilities
                GTest::gtest
                GTest::gtest_main
                mcd_util
                platform)
add_test(NAME audit_generator_unit_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND audit_generator_unit_test)
