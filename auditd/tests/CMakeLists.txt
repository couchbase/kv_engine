INCLUDE_DIRECTORIES(AFTER ${auditd_SOURCE_DIR}/src)
ADD_EXECUTABLE(memcached_timestamp_test timestamp_test.cc
               ${auditd_SOURCE_DIR}/src/isotime.h
               ${auditd_SOURCE_DIR}/src/isotime.cc)
ADD_TEST(memcached-timestamp-test memcached_timestamp_test)

ADD_EXECUTABLE(memcached_auditfile_test auditfile_test.cc
               ${auditd_SOURCE_DIR}/src/auditfile.h
               ${auditd_SOURCE_DIR}/src/auditfile.cc
               ${auditd_SOURCE_DIR}/src/isotime.h
               ${auditd_SOURCE_DIR}/src/isotime.cc)

SET_TARGET_PROPERTIES(memcached_auditfile_test
                      PROPERTIES COMPILE_FLAGS -DUNITTEST_AUDITFILE=1)
TARGET_LINK_LIBRARIES(memcached_auditfile_test dirutils cJSON)
ADD_TEST(memcached-auditfile-test memcached_auditfile_test)
