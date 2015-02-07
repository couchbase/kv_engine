PROJECT(auditd)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

IF (${CMAKE_MAJOR_VERSION} GREATER 2)
    CMAKE_POLICY(SET CMP0042 NEW)
ENDIF (${CMAKE_MAJOR_VERSION} GREATER 2)

INCLUDE_DIRECTORIES(BEFORE ${Platform_SOURCE_DIR}/include)

ADD_EXECUTABLE(auditevent_generator generator/auditevent_generator.c
                                    generator/auditevent_generator.h)
TARGET_LINK_LIBRARIES(auditevent_generator cJSON platform)

ADD_CUSTOM_COMMAND(OUTPUT ${auditd_BINARY_DIR}/audit_events.json
                  COMMAND
                     auditevent_generator
                            -r ${CMAKE_SOURCE_DIR}
                            -o ${auditd_BINARY_DIR}/audit_events.json
                            -c ${auditd_BINARY_DIR}/audit.json
                            -i ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json
                  DEPENDS
                        auditevent_generator
                        ${Memcached_SOURCE_DIR}/auditd/etc/module_descriptors.json
                        ${Memcached_SOURCE_DIR}/auditd/etc/auditd_descriptor.json
                        ${Memcached_SOURCE_DIR}/etc/memcached_descriptor.json
                  COMMENT "Generating audit descriptor definitions"
                  VERBATIM)

ADD_CUSTOM_TARGET(generate_audit_descriptors ALL
                  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/audit_events.json)

INSTALL(FILES
        ${auditd_BINARY_DIR}/audit_events.json
        DESTINATION etc/security)

INSTALL(FILES
        ${auditd_BINARY_DIR}/audit.json
        DESTINATION etc/security)

ADD_LIBRARY(auditd SHARED src/auditd.cc src/auditconfig.cc src/auditfile.cc src/audit.cc)
SET_TARGET_PROPERTIES(auditd PROPERTIES SOVERSION 0.1.0)
TARGET_LINK_LIBRARIES(auditd cJSON platform dirutils)

ADD_EXECUTABLE(memcached_auditd_tests src/testauditd.cc)
TARGET_LINK_LIBRARIES(memcached_auditd_tests auditd mcd_util cJSON dirutils)
ADD_TEST(memcached-auditd-test memcached_auditd_tests)

INSTALL(TARGETS auditd
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
