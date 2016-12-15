PROJECT(Memcached)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

IF (${CMAKE_MAJOR_VERSION} GREATER 2)
    CMAKE_POLICY(SET CMP0042 NEW)
ENDIF (${CMAKE_MAJOR_VERSION} GREATER 2)

INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckIncludeFiles)
INCLUDE(CheckIncludeFileCXX)
INCLUDE(CTest)
INCLUDE(GenerateExportHeader)

# The test program expects to find the output files in
# the root directory (that's how we built them earlier)
# let's continue to find them there until it's all done
# Unfortunately this means I need to add WORKING_DIRECTORY
# to ADD_TEST in order for it to correctly find the binary..
# (Please note that these settings only affect the BUILD
# directory structure, and not the installed directory
# structure
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

# Disable the build of Greenstack by default
OPTION(MEMCACHED_USE_GREENSTACK "Enable support for Greenstack" OFF)
IF (MEMCACHED_USE_GREENSTACK)
    ADD_DEFINITIONS(-DENABLE_GREENSTACK=1)
ENDIF (MEMCACHED_USE_GREENSTACK)

# Check if the system have a usable version of PKCS5_PBKDF2_HMAC_SHA1
CMAKE_PUSH_CHECK_STATE(RESET)
SET(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} ${OPENSSL_LIBRARIES})
SET(CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES} ${OPENSSL_INCLUDE_DIR})
CHECK_C_SOURCE_COMPILES("
         #include <openssl/evp.h>
         int main() {
             PKCS5_PBKDF2_HMAC_SHA1(NULL, 0, NULL, 0, 0, 0, NULL);
         }" HAVE_PKCS5_PBKDF2_HMAC_SHA1)
CHECK_C_SOURCE_COMPILES("
         #include <openssl/evp.h>
         int main() {
             PKCS5_PBKDF2_HMAC(NULL, 0, NULL, 0, 0, NULL, 0, NULL);
         }" HAVE_PKCS5_PBKDF2_HMAC)
CHECK_C_SOURCE_COMPILES("
         #include <openssl/ssl.h>
         int main() {
             long mask = SSL_OP_NO_TLSv1_1;
         }" HAVE_SSL_OP_NO_TLSv1_1)
CMAKE_POP_CHECK_STATE()

CMAKE_PUSH_CHECK_STATE(RESET)
CHECK_CXX_SOURCE_COMPILES("
   int main() {
       const char* func = __func__;
       return 1;
   }" HAVE_FUNC)

CHECK_CXX_SOURCE_COMPILES("
   int main() {
       const char* func = __FUNCTION__;
       return 1;
   }" HAVE_FUNCTION)
CMAKE_POP_CHECK_STATE()

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

IF (ENABLE_DTRACE)
    ADD_DEFINITIONS(-DENABLE_DTRACE=1)
ENDIF (ENABLE_DTRACE)

ADD_CUSTOM_COMMAND(OUTPUT ${Memcached_BINARY_DIR}/memcached_dtrace.h
                   COMMAND
                   ${DTRACE} -h
                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                   -o ${Memcached_BINARY_DIR}/memcached_dtrace.h
                   && sed -itmp "s/, *char \\*/, const char \\*/g" ${Memcached_BINARY_DIR}/memcached_dtrace.h
                   DEPENDS
                   memcached_dtrace.d
                   COMMENT "Generating DTrace probe header file"
                   VERBATIM)
ADD_CUSTOM_TARGET(generate_memcached_dtrace_h
                  DEPENDS ${Memcached_BINARY_DIR}/memcached_dtrace.h)

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

INCLUDE_DIRECTORIES(AFTER
                    ${gtest_SOURCE_DIR}/include
                    ${phosphor_SOURCE_DIR}/include)

INCLUDE_DIRECTORIES(AFTER ${PROJECT_BINARY_DIR}/include)

IF (MEMORY_ALLOCATOR)
    INCLUDE_DIRECTORIES(AFTER ${MALLOC_INCLUDE_DIR})
ELSE (MEMORY_ALLOCATOR)
    SET(MALLOC_LIBRARIES "")
ENDIF (MEMORY_ALLOCATOR)

IF (BREAKPAD_FOUND)
    ADD_DEFINITIONS(-DHAVE_BREAKPAD)
    INCLUDE_DIRECTORIES(AFTER ${BREAKPAD_INCLUDE_DIR})
    IF (UNIX)
        # On Linux Breakpad unit test relies on examining the minidump -> core
        # file with GDB.
        FIND_PROGRAM(GDB_EXECUTABLE gdb)
        IF (GDB_EXECUTABLE)
            # Two tests - crashing via segfault and uncaught exception
            ADD_TEST(memcached-breakpad-test-segfault ${PYTHON_EXECUTABLE}
                     ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
                     ${Memcached_BINARY_DIR}/memcached segfault ${MINIDUMP2CORE}
                     ${GDB_EXECUTABLE})

            ADD_TEST(memcached-breakpad-test-exception ${PYTHON_EXECUTABLE}
                     ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
                     ${Memcached_BINARY_DIR}/memcached exception ${MINIDUMP2CORE}
                     ${GDB_EXECUTABLE})
        ELSE (GDB_EXECUTABLE)
            MESSAGE(STATUS "GDB not available, skipping breakpad test")
        ENDIF (GDB_EXECUTABLE)

    # ELSEIF (WIN32)
    # Disable the test on Windows until we understand why we're seeing all
    # of the temporary test failures caused by python not detecting that
    # the processes died.
        # On Windows the test doesn't use minidump-2-core or GDB.
        # ADD_TEST(breakpad-test
        #          ${PYTHON_EXECUTABLE}
        #          ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
        #          ${Memcached_BINARY_DIR}/memcached)
    ENDIF (UNIX)
ENDIF (BREAKPAD_FOUND)


IF (MEMORY_ALLOCATOR STREQUAL jemalloc OR MEMORY_ALLOCATOR STREQUAL badmalloc)
    LIST(APPEND MEMORY_TRACKING_SRCS
         ${Memcached_SOURCE_DIR}/daemon/alloc_hooks_${MEMORY_ALLOCATOR}.cc)
ELSE ()
    LIST(APPEND MEMORY_TRACKING_SRCS
         ${Memcached_SOURCE_DIR}/daemon/alloc_hooks_dummy.cc)
ENDIF ()

LIST(APPEND MEMORY_TRACKING_SRCS ${Memcached_SOURCE_DIR}/daemon/alloc_hooks.h)

IF (APPLE)
    LIST(APPEND MEMORY_TRACKING_SRCS
         ${Memcached_SOURCE_DIR}/daemon/darwin_zone.cc
         ${Memcached_SOURCE_DIR}/daemon/darwin_zone.h)
ENDIF (APPLE)

ADD_LIBRARY(memory_tracking OBJECT ${MEMORY_TRACKING_SRCS})

IF (WIN32)
    INCLUDE_DIRECTORIES(AFTER ${Platform_SOURCE_DIR}/include/win32)
ENDIF (WIN32)

# We're using all of the sources to build up the server from one of
# the unit tests so let's just stash them in a variable
LIST(APPEND CBSASL_SOURCES
     ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/visibility.h
     ${Memcached_SOURCE_DIR}/cbsasl/cbsasl_internal.h
     ${Memcached_SOURCE_DIR}/cbsasl/client.cc
     ${Memcached_SOURCE_DIR}/cbsasl/common.cc
     ${Memcached_SOURCE_DIR}/cbsasl/log.cc
     ${Memcached_SOURCE_DIR}/cbsasl/mechanismfactory.cc
     ${Memcached_SOURCE_DIR}/cbsasl/mechanismfactory.h
     ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.cc
     ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.h
     ${Memcached_SOURCE_DIR}/cbsasl/password_database.cc
     ${Memcached_SOURCE_DIR}/cbsasl/password_database.h
     ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
     ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
     ${Memcached_SOURCE_DIR}/cbsasl/pwfile.cc
     ${Memcached_SOURCE_DIR}/cbsasl/pwfile.h
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/scram-sha.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/scram-sha.h
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.h
     ${Memcached_SOURCE_DIR}/cbsasl/server.cc
     ${Memcached_SOURCE_DIR}/cbsasl/strcmp.cc
     ${Memcached_SOURCE_DIR}/cbsasl/strerror.cc
     ${Memcached_SOURCE_DIR}/cbsasl/user.cc
     ${Memcached_SOURCE_DIR}/cbsasl/user.h
     ${Memcached_SOURCE_DIR}/cbsasl/util.h)

if (NOT WIN32)
    list(APPEND CBSASL_SOURCES
         ${Memcached_SOURCE_DIR}/cbsasl/plain/saslauthd.cc
         ${Memcached_SOURCE_DIR}/cbsasl/plain/saslauthd.h)
endif (NOT WIN32)

ADD_SUBDIRECTORY(etc)
ADD_SUBDIRECTORY(cbcrypto)
ADD_SUBDIRECTORY(cbsasl)
ADD_SUBDIRECTORY(time)
ADD_SUBDIRECTORY(support-files)
ADD_SUBDIRECTORY(protocol)
ADD_SUBDIRECTORY(programs)
ADD_SUBDIRECTORY(utilities)
ADD_SUBDIRECTORY(engines)
ADD_SUBDIRECTORY(extensions)
ADD_SUBDIRECTORY(testsuite)
ADD_SUBDIRECTORY(auditd)
ADD_SUBDIRECTORY(xattr)
ADD_SUBDIRECTORY(daemon)

CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp.pem COPYONLY)
CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp.cert COPYONLY)
# Second copy to test changing at runtime
CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp2.pem COPYONLY)
CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp2.cert COPYONLY)

ADD_SUBDIRECTORY(tests)

ENABLE_CODE_COVERAGE_REPORT()

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)
