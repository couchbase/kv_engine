cb_add_test_executable(memcached_audit_test
               audit_event_filter_test.cc
               auditfile_test.cc
               auditconfig_test.cc
               ${Memcached_SOURCE_DIR}/auditd/src/eventdescriptor.h)
cb_enable_unity_build(memcached_audit_test)
kv_enable_pch(memcached_audit_test)
target_link_libraries(memcached_audit_test PRIVATE
                      auditd
                      memcached_logger
                      memcached_rbac
                      platform
                      GTest::gtest
                      GTest::gtest_main
                      mcd_util)
target_include_directories(memcached_audit_test PRIVATE
        ${Memcached_SOURCE_DIR}/auditd/src
        ${Memcached_BINARY_DIR})
add_test(NAME memcached-audit-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_audit_test)
add_sanitizers(memcached_audit_test)
