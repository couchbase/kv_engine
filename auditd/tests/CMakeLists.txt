INCLUDE_DIRECTORIES(AFTER ${auditd_SOURCE_DIR}/src)
ADD_EXECUTABLE(memcached_auditfile_test auditfile_test.cc
               ${auditd_SOURCE_DIR}/src/auditconfig.h
               ${auditd_SOURCE_DIR}/src/auditconfig.cc
               ${auditd_SOURCE_DIR}/src/auditfile.h
               ${auditd_SOURCE_DIR}/src/auditfile.cc
               ${Memcached_SOURCE_DIR}/include/memcached/isotime.h)
SET_TARGET_PROPERTIES(memcached_auditfile_test
                      PROPERTIES COMPILE_FLAGS -DUNITTEST_AUDITFILE=1)
TARGET_LINK_LIBRARIES(memcached_auditfile_test mcd_time dirutils cJSON)
ADD_TEST(memcached-auditfile-test memcached_auditfile_test)

ADD_EXECUTABLE(memcached_auditconfig_test auditconfig_test.cc
               ${auditd_SOURCE_DIR}/src/auditconfig.h
               ${auditd_SOURCE_DIR}/src/auditconfig.cc)
TARGET_LINK_LIBRARIES(memcached_auditconfig_test dirutils cJSON)
ADD_TEST(memcached-auditconfig-test memcached_auditconfig_test)
