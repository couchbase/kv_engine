INCLUDE_DIRECTORIES(AFTER ${auditd_SOURCE_DIR}/src)
ADD_EXECUTABLE(memcached_timestamp_test timestamp_test.cc
               ${auditd_SOURCE_DIR}/src/isotime.h
               ${auditd_SOURCE_DIR}/src/isotime.cc)
ADD_TEST(memcached-timestamp-test memcached_timestamp_test)
