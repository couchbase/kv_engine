add_library(auditd STATIC
            audit.cc audit.h
            auditconfig.cc auditconfig.h
            audit_event_filter.cc audit_event_filter.h
            audit_interface.cc
            auditfile.cc auditfile.h
            configureevent.cc configureevent.h
            event.cc event.h
            eventdescriptor.cc
            eventdescriptor.h)
target_link_libraries(auditd
                      memcached_logger
                      mcd_time
                      mcd_util
                      platform
                      statistics
                      cbsocket)
target_include_directories(auditd PRIVATE ${Memcached_BINARY_DIR})
add_dependencies(auditd generate_audit_descriptors)


