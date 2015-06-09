ADD_CUSTOM_COMMAND(OUTPUT
                     ${Memcached_BINARY_DIR}/default_rbac.cc
                  COMMAND
                     generate_rbac -o ${Memcached_BINARY_DIR}/default_rbac.cc
                                   -i ${Memcached_SOURCE_DIR}/daemon/rbac.json
                  DEPENDS
                        rbac.json
                        generate_rbac
                  COMMENT "Generating code for default rbac config")

IF (BREAKPAD_FOUND AND UNIX)
  # Breakpad has headers (which are dependencies for our breakpad integration
  # code) that don't compile cleanly as C++11, so the GNU standard is
  # used instead.
  SET_SOURCE_FILES_PROPERTIES(breakpad.cc PROPERTIES COMPILE_FLAGS
                              ${CB_GNU_CXX11_OPTION})
ENDIF (BREAKPAD_FOUND AND UNIX)

ADD_EXECUTABLE(memcached
               breakpad.cc
               ${MEMORY_TRACKING_SRCS}
               cmdline.c
               cmdline.h
               config_util.c
               config_util.h
               config_parse.c
               config_parse.h
               connections.c
               connections.h
               debug_helpers.cc
               debug_helpers.h
               enginemap.h
               enginemap.cc
               hash.c
               ioctl.c
               mcaudit.c
               mcaudit.h
               mcbp_validators.cc
               mcbp_validators.h
               memcached.cc
               privileges.c
               subdocument.cc
               thread.c
               timings.cc
               mc_time.c
               rbac.cc
               rbac.h
               runtime.cc
               runtime.h
               ${Memcached_SOURCE_DIR}/utilities/protocol2text.c
               ${Memcached_BINARY_DIR}/default_rbac.cc)

ADD_DEPENDENCIES(memcached generate_audit_descriptors)

IF (ENABLE_DTRACE)
   ADD_DEPENDENCIES(memcached generate_memcached_dtrace_h)
   IF (DTRACE_NEED_INSTUMENT)
      ADD_CUSTOM_COMMAND(TARGET memcached PRE_LINK
                         COMMAND
                         ${DTRACE} -o memcached_dtrace.o
                                   -G
                                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                                   *.o
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/CMakeFiles/memcached.dir/daemon)
      SET_TARGET_PROPERTIES(memcached PROPERTIES LINK_FLAGS
                            "${Memcached_BINARY_DIR}/CMakeFiles/memcached.dir/daemon/memcached_dtrace.o")


   ENDIF (DTRACE_NEED_INSTUMENT)
ENDIF (ENABLE_DTRACE)

TARGET_LINK_LIBRARIES(memcached auditd mcd_util cbsasl platform cJSON JSON_checker subjson ${SNAPPY_LIBRARIES} ${MALLOC_LIBRARIES} ${LIBEVENT_LIBRARIES} ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS} ${BREAKPAD_LIBRARIES})
APPEND_MALLOC_LINKER_FLAGS(memcached)

INSTALL(TARGETS memcached
        RUNTIME DESTINATION bin)
