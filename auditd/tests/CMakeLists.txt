INCLUDE_DIRECTORIES(AFTER ${Memcached_SOURCE_DIR}/auditd/src)
ADD_EXECUTABLE(memcached_auditfile_test auditfile_test.cc
               ${Memcached_SOURCE_DIR}/include/memcached/isotime.h)
SET_TARGET_PROPERTIES(memcached_auditfile_test
                      PROPERTIES COMPILE_FLAGS -DUNITTEST_AUDITFILE=1)
TARGET_LINK_LIBRARIES(memcached_auditfile_test auditd memcached_logger mcd_time
                      dirutils platform gtest gtest_main
                      mcd_util)
ADD_TEST(NAME memcached-auditfile-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_auditfile_test)
add_sanitizers(memcached_auditfile_test)

ADD_EXECUTABLE(memcached_auditconfig_test auditconfig_test.cc)
TARGET_LINK_LIBRARIES(memcached_auditconfig_test auditd memcached_logger dirutils
                      platform gtest gtest_main mcd_util)
ADD_TEST(NAME memcached-auditconfig-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_auditconfig_test)
add_sanitizers(memcached_auditconfig_test)

ADD_EXECUTABLE(memcached_auditd_tests testauditd.cc)
TARGET_LINK_LIBRARIES(memcached_auditd_tests
                      auditd memcached_logger mcd_util mcd_time dirutils gtest)
ADD_DEPENDENCIES(memcached_auditd_tests generate_audit_descriptors)
add_sanitizers(memcached_auditd_tests)
ADD_TEST(NAME memcached-auditd-test
         COMMAND memcached_auditd_tests -e ${Memcached_BINARY_DIR}/auditd)

ADD_EXECUTABLE(memcached_audit_evdescr_test eventdescriptor_test.cc
               ${Memcached_SOURCE_DIR}/auditd/src/eventdescriptor.cc
               ${Memcached_SOURCE_DIR}/auditd/src/eventdescriptor.h)
TARGET_LINK_LIBRARIES(memcached_audit_evdescr_test gtest gtest_main mcd_util)
add_sanitizers(memcached_audit_evdescr_test)
ADD_TEST(NAME memcached-audit-evdescr-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_audit_evdescr_test)
