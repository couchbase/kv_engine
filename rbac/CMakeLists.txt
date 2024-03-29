add_library(memcached_rbac STATIC
            privileges.cc
            privilege_database.cc
            ${PROJECT_SOURCE_DIR}/include/memcached/rbac/privilege_database.h
            ${PROJECT_SOURCE_DIR}/include/memcached/rbac.h
            )
kv_enable_pch(memcached_rbac)
cb_enable_unity_build(memcached_rbac)

target_link_libraries(memcached_rbac
    PUBLIC platform
    PRIVATE cbsasl)

cb_add_test_executable(memcached_rbac_test
               privilege_test.cc
               privilege_database_test.cc)
cb_enable_unity_build(memcached_rbac_test)
kv_enable_pch(memcached_rbac_test)
target_link_libraries(memcached_rbac_test
    PRIVATE Folly::headers memcached_rbac GTest::gtest GTest::gtest_main)
add_test(NAME memcached-rbac-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_rbac_test)
add_sanitizers(memcached_rbac_test)

cb_add_test_executable(privilege_check_bench privilege_check_bench.cc)
target_link_libraries(privilege_check_bench PRIVATE
        Folly::headers benchmark::benchmark memcached_rbac)
