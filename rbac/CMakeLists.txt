add_library(memcached_rbac STATIC
            privileges.cc
            privilege_database.cc
            ${PROJECT_SOURCE_DIR}/include/memcached/rbac/privilege_database.h
            ${PROJECT_SOURCE_DIR}/include/memcached/rbac.h
            )

target_link_libraries(memcached_rbac cbsasl cJSON platform)
