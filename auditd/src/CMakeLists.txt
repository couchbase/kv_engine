add_library(auditd STATIC
            audit.cc audit.h
            audit_descriptor_manager.cc audit_descriptor_manager.h
            auditconfig.cc auditconfig.h
            audit_event_filter.cc audit_event_filter.h
            audit_interface.cc
            auditfile.cc auditfile.h
            configureevent.cc configureevent.h
            event.cc event.h
            eventdescriptor.h)
add_dependencies(auditd generate_audit_descriptors)
kv_enable_pch(auditd)
target_link_libraries(auditd PRIVATE
                      memcached_logger
                      mcd_dek
                      mcd_util
                      platform
                      statistics
                      cbsocket)
target_include_directories(auditd PRIVATE ${Memcached_BINARY_DIR})
cb_enable_unity_build(auditd)
add_dependencies(auditd generate_audit_descriptors)
kv_enable_pch(auditd)
