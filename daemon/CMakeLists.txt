INCLUDE_DIRECTORIES(AFTER ${Greenstack_SOURCE_DIR/include})

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
               ${Memcached_SOURCE_DIR}/utilities/protocol2text.cc
               ${Memcached_SOURCE_DIR}/utilities/string_utilities.cc
               ${Memcached_SOURCE_DIR}/utilities/terminate_handler.cc
               $<TARGET_OBJECTS:memory_tracking>
               breakpad.h
               buckets.cc
               buckets.h
               buffer.h
               cmdline.cc
               cmdline.h
               config_parse.cc
               config_parse.h
               config_util.cc
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
               cookie.h
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
               libevent_locking.cc
               libevent_locking.h
               log_macros.h
               mc_time.cc
               mc_time.h
               mcaudit.cc
               mcaudit.h
               mcbp.cc
               mcbp.h
               mcbp_executors.cc
               mcbp_executors.h
               mcbp_privileges.cc
               mcbp_privileges.h
               mcbp_topkeys.cc
               mcbp_topkeys.h
               mcbp_validators.cc
               mcbp_validators.h
               mcbpdestroybuckettask.h
               memcached.cc
               memcached_openssl.cc
               memcached_openssl.h
               net_buf.h
               parent_monitor.cc
               parent_monitor.h
               protocol/mcbp/appendprepend_context.cc
               protocol/mcbp/appendprepend_context.h
               protocol/mcbp/arithmetic_context.cc
               protocol/mcbp/arithmetic_context.h
               runtime.cc
               runtime.h
               sasl_tasks.cc
               sasl_tasks.h
               session_cas.cc
               session_cas.h
               settings.cc
               settings.h
               ssl_utils.cc
               ssl_utils.h
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
               topkeys.h
               tracing.cc
               tracing.h)

ADD_DEPENDENCIES(memcached_daemon generate_audit_descriptors)

IF (NOT WIN32)
    LIST(APPEND MEMCACHED_EXTRA_LIBS ${LIBEVENT_THREAD_LIB})
ENDIF (NOT WIN32)

TARGET_LINK_LIBRARIES(memcached_daemon
                      auditd
                      mcd_util
                      greenstack
                      cbsasl
                      cbcompress
                      platform
                      cJSON
                      JSON_checker
                      subjson
                      phosphor
                      ${SNAPPY_LIBRARIES}
                      ${MALLOC_LIBRARIES}
                      ${LIBEVENT_CORE_LIB}
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS}
                      ${BREAKPAD_LIBRARIES}
                      ${NUMA_LIBRARIES}
                      ${MEMCACHED_EXTRA_LIBS})

ADD_EXECUTABLE(memcached main.cc)
TARGET_LINK_LIBRARIES(memcached memcached_daemon)

IF (ENABLE_DTRACE)
   ADD_DEPENDENCIES(memcached_daemon generate_memcached_dtrace_h)
   IF (DTRACE_NEED_INSTRUMENT)
      ADD_CUSTOM_COMMAND(TARGET memcached_daemon PRE_LINK
                         COMMAND
                         ${DTRACE} -o memcached_dtrace.o
                                   -G
                                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                                   *.o
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/daemon/CMakeFiles/memcached_daemon.dir)
      SET_TARGET_PROPERTIES(memcached PROPERTIES LINK_FLAGS
                            "${Memcached_BINARY_DIR}/daemon/CMakeFiles/memcached_daemon.dir/memcached_dtrace.o")


   ENDIF (DTRACE_NEED_INSTRUMENT)
ENDIF (ENABLE_DTRACE)

INSTALL(TARGETS memcached
        RUNTIME DESTINATION bin)
