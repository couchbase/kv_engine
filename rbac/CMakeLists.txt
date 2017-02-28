add_library(memcached_rbac SHARED
            privileges.cc
            privilege_database.cc
            ${PROJECT_SOURCE_DIR}/include/memcached/rbac/privilege_database.h
            ${PROJECT_SOURCE_DIR}/include/memcached/rbac.h
            )

set_target_properties(memcached_rbac PROPERTIES SOVERSION 1.0.0)
target_link_libraries(memcached_rbac cbsasl cJSON platform)

generate_export_header(memcached_rbac
                       EXPORT_MACRO_NAME RBAC_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/memcached/rbac/visibility.h)

install(TARGETS memcached_rbac
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
