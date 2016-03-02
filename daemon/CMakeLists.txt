INCLUDE_DIRECTORIES(AFTER ${Greenstack_SOURCE_DIR/include})
ADD_CUSTOM_COMMAND(OUTPUT
                     ${Memcached_BINARY_DIR}/default_rbac.cc
                  COMMAND
                     generate_rbac -o ${Memcached_BINARY_DIR}/default_rbac.cc
                                   -i ${Memcached_SOURCE_DIR}/daemon/rbac.json
                  DEPENDS
                        rbac.json
                        generate_rbac
                  COMMENT "Generating code for default rbac config")

IF (BREAKPAD_FOUND)
  IF (WIN32)
    SET(BREAKPAD_SRCS breakpad_win32.cc)
  ELSEIF ("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
    SET(BREAKPAD_SRCS breakpad_linux.cc)
    # Breakpad has headers (which are dependencies for our breakpad integration
    # code) that don't compile cleanly as C++11, so the GNU standard is
    # used instead.
    SET_SOURCE_FILES_PROPERTIES(breakpad_linux.cc PROPERTIES COMPILE_FLAGS
                                ${CB_GNU_CXX11_OPTION})
  ELSE()
    MESSAGE(STATUS "Breakpad not supported on this platform")
    SET(BREAKPAD_SRCS breakpad_dummy.cc)
  ENDIF()
ELSE()
  SET(BREAKPAD_SRCS breakpad_dummy.cc)
ENDIF()

CHECK_INCLUDE_FILES(numa.h HAVE_NUMA_H)
SET(WITH_NUMA True CACHE BOOL "Explicitly set NUMA memory allocation policy")
IF(HAVE_NUMA_H AND WITH_NUMA)
   CMAKE_PUSH_CHECK_STATE(RESET)
      SET(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} numa)
      CHECK_C_SOURCE_COMPILES("
         #include <numa.h>
         int main() {
            numa_available();
         }" HAVE_LIBNUMA)
   CMAKE_POP_CHECK_STATE()
ENDIF()
IF(HAVE_LIBNUMA)
   SET(NUMA_LIBRARIES numa)
ENDIF()

ADD_LIBRARY(memcached_daemon STATIC
               ${BREAKPAD_SRCS}
               ${Memcached_BINARY_DIR}/default_rbac.cc
               ${Memcached_SOURCE_DIR}/utilities/protocol2text.cc
               ${MEMORY_TRACKING_SRCS}
               breakpad.h
               buckets.cc
               buckets.h
               buffer.h
               cmdline.cc
               cmdline.h
               config_parse.cc
               config_parse.h
               config_util.c
               config_util.h
               connection.cc
               connection.h
               connection_greenstack.cc
               connection_greenstack.h
               connection_listen.cc
               connection_listen.h
               connection_mcbp.cc
               connection_mcbp.h
               connections.cc
               connections.h
               debug_helpers.cc
               debug_helpers.h
               dynamic_buffer.cc
               dynamic_buffer.h
               enginemap.cc
               enginemap.h
               executor.cc
               executor.h
               executorpool.cc
               executorpool.h
               greenstack.cc
               greenstack.h
               ioctl.cc
               ioctl.h
               log_macros.h
               mc_time.cc
               mc_time.h
               mcaudit.cc
               mcaudit.h
               mcbp.cc
               mcbp.h
               mcbp_executors.cc
               mcbp_executors.h
               mcbp_topkeys.cc
               mcbp_topkeys.h
               mcbp_validators.cc
               mcbp_validators.h
               memcached.cc
               memcached_openssl.cc
               memcached_openssl.h
               net_buf.h
               privileges.c
               privileges.h
               rbac.cc
               rbac.h
               rbac_impl.h
               runtime.cc
               runtime.h
               sasl_tasks.cc
               sasl_tasks.h
               session_cas.cc
               session_cas.h
               settings.h
               statemachine_mcbp.cc
               statemachine_mcbp.h
               stats.h
               subdocument.cc
               subdocument.h
               subdocument_context.h
               subdocument_traits.cc
               subdocument_traits.h
               subdocument_validators.cc
               subdocument_validators.h
               task.h
               task.cc
               thread.cc
               timing_histogram.cc
               timing_histogram.h
               timings.cc
               timings.h
               topkeys.cc
               topkeys.h)

ADD_DEPENDENCIES(memcached_daemon generate_audit_descriptors)

IF (ENABLE_DTRACE)
   ADD_DEPENDENCIES(memcached_daemon generate_memcached_dtrace_h)
   IF (DTRACE_NEED_INSTUMENT)
      ADD_CUSTOM_COMMAND(TARGET memcached_daemon PRE_LINK
                         COMMAND
                         ${DTRACE} -o memcached_dtrace.o
                                   -G
                                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                                   *.o
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/daemon/CMakeFiles/memcached_daemon.dir)
      SET_TARGET_PROPERTIES(memcached PROPERTIES LINK_FLAGS
                            "${Memcached_BINARY_DIR}/daemon/CMakeFiles/memcached_daemon.dir/memcached_dtrace.o")


   ENDIF (DTRACE_NEED_INSTUMENT)
ENDIF (ENABLE_DTRACE)

TARGET_LINK_LIBRARIES(memcached_daemon
                      auditd
                      mcd_util
                      greenstack
                      cbsasl
                      platform
                      cJSON
                      JSON_checker
                      subjson
                      ${SNAPPY_LIBRARIES}
                      ${MALLOC_LIBRARIES}
                      ${LIBEVENT_LIBRARIES}
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS}
                      ${BREAKPAD_LIBRARIES}
                      ${NUMA_LIBRARIES})

ADD_EXECUTABLE(memcached main.cc)
TARGET_LINK_LIBRARIES(memcached memcached_daemon)

APPEND_MALLOC_LINKER_FLAGS(memcached)

INSTALL(TARGETS memcached
        RUNTIME DESTINATION bin)
