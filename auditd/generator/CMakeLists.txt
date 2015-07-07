ADD_EXECUTABLE(auditevent_generator auditevent_generator.cc
                                    auditevent_generator.h)
TARGET_LINK_LIBRARIES(auditevent_generator cJSON platform)

ADD_CUSTOM_COMMAND(OUTPUT
                     ${Memcached_BINARY_DIR}/auditd/audit_events.json
                     ${Memcached_BINARY_DIR}/auditd/auditd_audit_events.h
                  COMMAND
                     auditevent_generator
                            -r ${CMAKE_SOURCE_DIR}
                            -b ${CMAKE_BINARY_DIR}
                            -o ${Memcached_BINARY_DIR}/auditd/audit_events.json
                            -i ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json
                  DEPENDS
                        auditevent_generator
                        ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json
                        ${Memcached_SOURCE_DIR}/auditd/etc/auditd_descriptor.json
                        ${Memcached_SOURCE_DIR}/etc/memcached_descriptor.json
                  COMMENT "Generating audit descriptor definitions"
                  VERBATIM)

ADD_CUSTOM_TARGET(generate_audit_descriptors ALL
                  DEPENDS ${Memcached_BINARY_DIR}/auditd/audit_events.json)

INSTALL(FILES
        ${Memcached_BINARY_DIR}/auditd/audit_events.json
        DESTINATION etc/security)
