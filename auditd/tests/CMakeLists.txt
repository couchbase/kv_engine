include_directories(AFTER ${Memcached_SOURCE_DIR}/auditd/src)
cb_add_test_executable(memcached_audit_test
               auditfile_test.cc
               auditconfig_test.cc
               eventdescriptor_test.cc
               ${Memcached_SOURCE_DIR}/auditd/src/eventdescriptor.h
               ${Memcached_SOURCE_DIR}/include/memcached/isotime.h)
kv_enable_pch(memcached_audit_test)
target_link_libraries(memcached_audit_test PRIVATE
                      auditd
                      memcached_logger
                      mcd_time
                      platform
                      gtest
                      gtest_main
                      mcd_util)
add_test(NAME memcached-audit-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_audit_test)
add_sanitizers(memcached_audit_test)

cb_add_test_executable(memcached_auditd_tests testauditd.cc)
kv_enable_pch(memcached_auditd_tests)
target_link_libraries(memcached_auditd_tests PRIVATE
                      auditd memcached_logger mcd_util mcd_time platform gtest)
add_dependencies(memcached_auditd_tests generate_audit_descriptors)
add_sanitizers(memcached_auditd_tests)
add_test(NAME memcached-auditd-test
         COMMAND memcached_auditd_tests -e ${Memcached_BINARY_DIR}/auditd)
