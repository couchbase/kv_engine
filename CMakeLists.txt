PROJECT(Memcached)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

IF (${CMAKE_MAJOR_VERSION} GREATER 2)
    CMAKE_POLICY(SET CMP0042 NEW)
ENDIF (${CMAKE_MAJOR_VERSION} GREATER 2)

INCLUDE(CheckIncludeFileCXX)
INCLUDE(CTest)

IF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
   EXECUTE_PROCESS(COMMAND git log -1 --pretty=format:%H
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                   OUTPUT_VARIABLE MEMCACHED_VERSION
                   ERROR_QUIET
                   OUTPUT_STRIP_TRAILING_WHITESPACE)
ELSE (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
   SET(MEMCACHED_VERSION "unknown")
ENDIF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)

CHECK_SYMBOL_EXISTS(memalign malloc.h HAVE_MEMALIGN)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)

IF (ENABLE_DTRACE)
   ADD_DEFINITIONS(-DENABLE_DTRACE=1)
ENDIF (ENABLE_DTRACE)

ADD_CUSTOM_COMMAND(OUTPUT ${Memcached_BINARY_DIR}/memcached_dtrace.h
                   COMMAND
                     ${DTRACE} -h
                               -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                               -o ${Memcached_BINARY_DIR}/memcached_dtrace.h
                   DEPENDS
                         memcached_dtrace.d
                   COMMENT "Generating DTrace probe header file"
                   VERBATIM)

INCLUDE_DIRECTORIES(BEFORE
                    ${LIBEVENT_INCLUDE_DIR}
                    ${SNAPPY_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${CMAKE_INSTALL_PREFIX}/include
                    ${CMAKE_CURRENT_SOURCE_DIR}/include
                    ${CMAKE_CURRENT_BINARY_DIR}
                    ${CMAKE_CURRENT_SOURCE_DIR}
                    ${Platform_SOURCE_DIR}/include
                    ${subjson_SOURCE_DIR})

IF (MEMORY_ALLOCATOR)
   INCLUDE_DIRECTORIES(AFTER ${MALLOC_INCLUDE_DIR})
ELSE (MEMORY_ALLOCATOR)
   SET(MALLOC_LIBRARIES "")
ENDIF (MEMORY_ALLOCATOR)

FUNCTION(APPEND_MALLOC_LINKER_FLAGS _target)
   IF (("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "tcmalloc") AND WIN32)
      SET(__ldflag "/INCLUDE:__tcmalloc")
      GET_TARGET_PROPERTY(__existing_ldflags ${_target} LINK_FLAG)
      IF (__existing_ldflags)
         SET(__ldflags "${__existing_ldflags} ${__ldflags}")
      ENDIF (__existing_ldflags)
      SET_TARGET_PROPERTIES(memcached PROPERTIES LINK_FLAG "${__memcached_ldflags}")
   ENDIF (("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "tcmalloc") AND WIN32)
ENDFUNCTION()

IF (BREAKPAD_FOUND)
   ADD_DEFINITIONS(-DHAVE_BREAKPAD)
   INCLUDE_DIRECTORIES(AFTER ${BREAKPAD_INCLUDE_DIR})
   # Breakpad has headers (which are dependencies for our breakpad integration
   # code) that don't compile cleanly as C++11, so the GNU standard is
   # used instead.
   IF (UNIX)
      set_source_files_properties(daemon/breakpad.cc PROPERTIES COMPILE_FLAGS ${CB_GNU_CXX11_OPTION})

      # On Linux Breakpad unit test relies on examining the minidump -> core
      # file with GDB.
      FIND_PROGRAM(GDB_EXECUTABLE gdb)
      IF (GDB_EXECUTABLE)
         ADD_TEST(breakpad-test ${PYTHON_EXECUTABLE}
                  ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
                  ${Memcached_BINARY_DIR}/memcached ${MINIDUMP2CORE} ${GDB_EXECUTABLE})
      ENDIF (GDB_EXECUTABLE)

   ELSEIF (WIN32)
      # On Windows the test doesn't use minidump-2-core or GDB.
      ADD_TEST(breakpad-test
               ${PYTHON_EXECUTABLE} ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
               ${Memcached_BINARY_DIR}/memcached)

   ENDIF (UNIX)

ENDIF (BREAKPAD_FOUND)

IF (APPLE)
   SET(MEMORY_TRACKING_SRCS ${Memcached_SOURCE_DIR}/daemon/alloc_hooks.c
                            ${Memcached_SOURCE_DIR}/daemon/alloc_hooks.h
                            ${Memcached_SOURCE_DIR}/daemon/darwin_zone.c
                            ${Memcached_SOURCE_DIR}/daemon/darwin_zone.h)
ELSE (APPLE)
   SET(MEMORY_TRACKING_SRCS ${Memcached_SOURCE_DIR}/daemon/alloc_hooks.c
                            ${Memcached_SOURCE_DIR}/daemon/alloc_hooks.h)
ENDIF (APPLE)

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${Platform_SOURCE_DIR}/include/win32)
ENDIF (WIN32)

ADD_SUBDIRECTORY(etc)
ADD_SUBDIRECTORY(time)
ADD_SUBDIRECTORY(support-files)
ADD_SUBDIRECTORY(programs)

#
# Add all of the libraries
#
ADD_LIBRARY(mcd_util SHARED
            utilities/config_parser.c
            utilities/engine_loader.c
            utilities/extension_loggers.c
            utilities/protocol2text.c
            utilities/util.c)
ADD_LIBRARY(default_engine SHARED
            engines/default_engine/assoc.c
            engines/default_engine/default_engine.c
            engines/default_engine/items.c
            engines/default_engine/slabs.c)
ADD_LIBRARY(nobucket SHARED
            engines/nobucket/nobucket.c)
ADD_LIBRARY(bucket_engine SHARED
            engines/bucket_engine/bucket_engine.c
            engines/bucket_engine/topkeys.c
            engines/bucket_engine/genhash.c
            utilities/engine_loader.c)
ADD_LIBRARY(crash_engine SHARED
            engines/crash_engine/crash_engine.c)
ADD_LIBRARY(ewouldblock_engine SHARED
            engines/ewouldblock_engine/ewouldblock_engine.cc)

ADD_LIBRARY(basic_engine_testsuite SHARED testsuite/basic_engine_testsuite.c)
ADD_LIBRARY(blackhole_logger SHARED extensions/loggers/blackhole_logger.c)
ADD_LIBRARY(stdin_term_handler SHARED extensions/daemon/stdin_check.c)
ADD_LIBRARY(tap_mock_engine SHARED engines/tap_mock_engine/tap_mock_engine.cc)
ADD_LIBRARY(bucket_engine_mock_engine SHARED
            engines/bucket_engine/mock_engine.c
            engines/bucket_engine/genhash.c)
ADD_LIBRARY(file_logger SHARED extensions/loggers/file_logger.cc)
ADD_LIBRARY(testapp_extension SHARED extensions/protocol/testapp_extension.c)

ADD_SUBDIRECTORY(auditd)

SET_TARGET_PROPERTIES(default_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(nobucket PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(bucket_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(crash_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(ewouldblock_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(basic_engine_testsuite PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(stdin_term_handler PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(tap_mock_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(bucket_engine_mock_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(file_logger PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(testapp_extension PROPERTIES PREFIX "")

#
# All of the binaries we're building
#
ADD_EXECUTABLE(bucket_engine_testapp
               engines/bucket_engine/testapp.cc
               engines/bucket_engine/genhash.c
               utilities/engine_loader.c)
ADD_EXECUTABLE(engine_testapp programs/engine_testapp/engine_testapp.c
                              programs/engine_testapp/mock_server.c
                              programs/engine_testapp/mock_server.h
                              ${MEMORY_TRACKING_SRCS})

ADD_EXECUTABLE(generate_rbac programs/generate_rbac/generate_rbac.c)
TARGET_LINK_LIBRARIES(generate_rbac platform)


ADD_CUSTOM_COMMAND(OUTPUT
                     ${Memcached_BINARY_DIR}/default_rbac.cc
                  COMMAND
                     generate_rbac -o ${Memcached_BINARY_DIR}/default_rbac.cc
                                   -i ${Memcached_SOURCE_DIR}/daemon/rbac.json
                  DEPENDS
                        daemon/rbac.json
                        generate_rbac
                  COMMENT "Generating code for default rbac config")

ADD_EXECUTABLE(memcached
               daemon/breakpad.cc
               ${MEMORY_TRACKING_SRCS}
               daemon/cmdline.c
               daemon/cmdline.h
               daemon/config_util.c
               daemon/config_util.h
               daemon/config_parse.c
               daemon/config_parse.h
               daemon/connections.c
               daemon/connections.h
               daemon/debug_helpers.cc
               daemon/debug_helpers.h
               daemon/hash.c
               daemon/ioctl.c
               daemon/mcaudit.c
               daemon/mcaudit.h
               daemon/mcbp_validators.cc
               daemon/mcbp_validators.h
               daemon/memcached.c
               daemon/privileges.c
               daemon/subdocument.cc
               daemon/stats.c
               daemon/thread.c
               daemon/timings.cc
               daemon/mc_time.c
               daemon/rbac.cc
               daemon/rbac.h
               daemon/runtime.cc
               daemon/runtime.h
               utilities/protocol2text.c
               ${Memcached_BINARY_DIR}/default_rbac.cc)
ADD_DEPENDENCIES(memcached generate_audit_descriptors)

IF (ENABLE_DTRACE)
   ADD_CUSTOM_TARGET(generate_memcached_dtrace_h
                     DEPENDS ${Memcached_BINARY_DIR}/memcached_dtrace.h)
   ADD_DEPENDENCIES(memcached generate_memcached_dtrace_h)
   ADD_DEPENDENCIES(default_engine generate_memcached_dtrace_h)

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

      ADD_CUSTOM_COMMAND(TARGET default_engine PRE_LINK
                         COMMAND
                         ${DTRACE} -o de_dtrace.o
                                   -G
                                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                                   *.o
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/CMakeFiles/default_engine.dir/engines/default_engine)
      SET_TARGET_PROPERTIES(default_engine PROPERTIES LINK_FLAGS
                            "${Memcached_BINARY_DIR}/CMakeFiles/default_engine.dir/engines/default_engine/de_dtrace.o")

   ENDIF (DTRACE_NEED_INSTUMENT)
ENDIF (ENABLE_DTRACE)


ADD_EXECUTABLE(memcached_testapp
               tests/testapp.cc tests/testapp.h
               tests/testapp_subdoc.cc
               programs/utilities.c)

SET(CBSASL_SOURCES
  ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
  ${Memcached_SOURCE_DIR}/include/cbsasl/visibility.h
  ${Memcached_SOURCE_DIR}/cbsasl/client.c
  ${Memcached_SOURCE_DIR}/cbsasl/common.c
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/cram-md5.c
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/cram-md5.h
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/hmac.c
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/hmac.h
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/md5.c
  ${Memcached_SOURCE_DIR}/cbsasl/cram-md5/md5.h
  ${Memcached_SOURCE_DIR}/cbsasl/hash.c
  ${Memcached_SOURCE_DIR}/cbsasl/hash.h
  ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.c
  ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.h
  ${Memcached_SOURCE_DIR}/cbsasl/pwfile.c
  ${Memcached_SOURCE_DIR}/cbsasl/pwfile.h
  ${Memcached_SOURCE_DIR}/cbsasl/server.c
  ${Memcached_SOURCE_DIR}/cbsasl/strcmp.c
  ${Memcached_SOURCE_DIR}/cbsasl/util.h)

ADD_LIBRARY(cbsasl SHARED ${CBSASL_SOURCES})
SET_TARGET_PROPERTIES(cbsasl PROPERTIES SOVERSION 1.1.1)
SET_TARGET_PROPERTIES(cbsasl PROPERTIES COMPILE_FLAGS -DBUILDING_CBSASL=1)
TARGET_LINK_LIBRARIES(cbsasl platform)

#
# Add linker flags to all of the binaries
#
TARGET_LINK_LIBRARIES(bucket_engine mcd_util platform ${COUCHBASE_NETWORK_LIBS} ${COUCHBASE_MATH_LIBS})
TARGET_LINK_LIBRARIES(default_engine mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(ewouldblock_engine mcd_util)

TARGET_LINK_LIBRARIES(basic_engine_testsuite mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(stdin_term_handler platform)
TARGET_LINK_LIBRARIES(engine_testapp mcd_util platform ${MALLOC_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
APPEND_MALLOC_LINKER_FLAGS(engine_testapp)
TARGET_LINK_LIBRARIES(bucket_engine_testapp mcd_util platform ${COUCHBASE_NETWORK_LIBS} ${COUCHBASE_MATH_LIBS})
TARGET_LINK_LIBRARIES(tap_mock_engine platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(testapp_extension mcd_util platform ${COUCHBASE_NETWORK_LIBS})

TARGET_LINK_LIBRARIES(mcd_util platform)
TARGET_LINK_LIBRARIES(memcached auditd mcd_util cbsasl platform cJSON JSON_checker subjson ${SNAPPY_LIBRARIES} ${MALLOC_LIBRARIES} ${LIBEVENT_LIBRARIES} ${OPENSSL_LIBRARIES} ${COUCHBASE_NETWORK_LIBS} ${BREAKPAD_LIBRARIES})
APPEND_MALLOC_LINKER_FLAGS(memcached)

TARGET_LINK_LIBRARIES(memcached_testapp mcd_util cbsasl cJSON platform ${SNAPPY_LIBRARIES} ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS} ${OPENSSL_LIBRARIES})

TARGET_LINK_LIBRARIES(file_logger mcd_time platform)

IF (INSTALL_HEADER_FILES)
   INSTALL (FILES include/memcached/allocator_hooks.h
                  include/memcached/callback.h
                  include/memcached/config_parser.h
                  include/memcached/engine.h
                  include/memcached/engine_common.h
                  include/memcached/engine_testapp.h
                  include/memcached/extension.h
                  include/memcached/extension_loggers.h
                  include/memcached/protocol_binary.h
                  include/memcached/server_api.h
                  include/memcached/types.h
                  include/memcached/dcp.h
                  include/memcached/util.h
                  include/memcached/vbucket.h
                  include/memcached/visibility.h
            DESTINATION include/memcached)

   INSTALL(FILES
           include/cbsasl/cbsasl.h
           include/cbsasl/visibility.h
           DESTINATION include/cbsasl)
ENDIF(INSTALL_HEADER_FILES)

SET_TARGET_PROPERTIES(mcd_util PROPERTIES SOVERSION 1.0.0)
INSTALL(TARGETS mcd_util
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)

INSTALL(TARGETS default_engine nobucket bucket_engine blackhole_logger
                stdin_term_handler file_logger
        RUNTIME DESTINATION lib/memcached
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)

INSTALL(FILES daemon/rbac.json DESTINATION etc/security)
INSTALL(TARGETS engine_testapp memcached
        RUNTIME DESTINATION bin)

INSTALL(TARGETS cbsasl
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

ADD_TEST(memcached-basic-unit-tests-plain memcached_testapp plain)
ADD_TEST(memcached-basic-unit-tests-SSL memcached_testapp ssl)
ADD_TEST(memcached-bucket_engine-unit-tests bucket_engine_testapp)
ADD_TEST(memcached-basic-engine-tests engine_testapp -E default_engine.so -T basic_engine_testsuite.so)

IF(${COUCHBASE_PYTHON})
    FOREACH(ID RANGE 9)
        LIST(APPEND GENERATED_SRCS ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_${ID}.c)
    ENDFOREACH()

    ADD_CUSTOM_COMMAND(OUTPUT ${GENERATED_SRCS}
                       COMMAND
                           ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/testsuite/breakdancer/engine_test.py
                       DEPENDS
                           testsuite/breakdancer/engine_test.py
                           testsuite/breakdancer/breakdancer.py
                       COMMENT "Generating testsuite")

    # Dummy target to prevent cmake trying to run multiple instances of the
    # above "Generating testsuite" command. The actual shared libraries
    # below in turn depend on this, causing them to be 'blocked' behind this.
    ADD_CUSTOM_TARGET(DUMMY_generated_suite
                      DEPENDS ${GENERATED_SRCS}
                      COMMENT "Waiting for testsuite generation to complete")

    FOREACH(ID RANGE 9)
        ADD_LIBRARY(memcached_generated_testsuite_${ID} SHARED
                    testsuite/breakdancer/suite_stubs.c
                    ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_${ID}.c)
        SET_TARGET_PROPERTIES(memcached_generated_testsuite_${ID} PROPERTIES PREFIX "")
        ADD_DEPENDENCIES(memcached_generated_testsuite_${ID}
                         DUMMY_generated_suite)

        ADD_TEST(memcached-breakdancer-engine-tests_${ID}
                 engine_testapp -E default_engine.so -T memcached_generated_testsuite_${ID}.so)
    ENDFOREACH()
ENDIF(${COUCHBASE_PYTHON})

CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp.pem COPYONLY)
CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp.cert COPYONLY)
# Second copy to test changing at runtime
CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp2.pem COPYONLY)
CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp2.cert COPYONLY)

ADD_SUBDIRECTORY(tests)

ADD_CUSTOM_TARGET(memcached-manpage
                  COMMAND a2x -L --doctype manpage --format manpage
                          ${CMAKE_CURRENT_SOURCE_DIR}/man/man4/memcached.json.4.txt
                  COMMAND a2x -L --doctype manpage --format pdf
                          ${CMAKE_CURRENT_SOURCE_DIR}/man/man4/memcached.json.4.txt
                  VERBATIM)

IF (NOT WIN32)
   INSTALL(FILES man/man4/memcached.json.4
           DESTINATION man/man4)
ENDIF(NOT WIN32)

INSTALL(FILES man/man4/memcached.json.4.pdf
        DESTINATION doc)

IF (${CMAKE_MAJOR_VERSION} LESS 3)
    SET_TARGET_PROPERTIES(mcd_util PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib/memcached)
    SET_TARGET_PROPERTIES(default_engine PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib/memcached)
    SET_TARGET_PROPERTIES(bucket_engine PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib/memcached)
    SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib/memcached)
    SET_TARGET_PROPERTIES(stdin_term_handler PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib/memcached)
    SET_TARGET_PROPERTIES(file_logger PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib/memcached)
    SET_TARGET_PROPERTIES(auditd PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib/memcached)
    SET_TARGET_PROPERTIES(cbsasl PROPERTIES INSTALL_NAME_DIR
                          ${CMAKE_INSTALL_PREFIX}/lib)
ENDIF (${CMAKE_MAJOR_VERSION} LESS 3)

ENABLE_CODE_COVERAGE_REPORT()
