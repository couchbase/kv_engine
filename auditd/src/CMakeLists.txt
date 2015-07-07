ADD_LIBRARY(auditd SHARED
            audit.cc audit.h
            auditconfig.cc auditconfig.h
            auditd.cc auditd.h
            auditfile.cc auditfile.h
            configureevent.cc configureevent.h
            event.cc event.h
            eventdata.h)
SET_TARGET_PROPERTIES(auditd PROPERTIES SOVERSION 0.1.0)
TARGET_LINK_LIBRARIES(auditd mcd_time cJSON JSON_checker platform dirutils)
ADD_DEPENDENCIES(auditd generate_audit_descriptors)

INSTALL(TARGETS auditd
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
