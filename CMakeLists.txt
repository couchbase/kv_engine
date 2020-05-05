CMAKE_MINIMUM_REQUIRED(VERSION 3.2)
PROJECT(Memcached LANGUAGES C CXX)

INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckIncludeFiles)
INCLUDE(CheckIncludeFileCXX)
INCLUDE(CTest)
INCLUDE(GenerateExportHeader)
include(check_unit_test_enabled)
include(HeaderObjectLibrary)

check_unit_test_enabled(kv_engine COUCHBASE_KV_BUILD_UNIT_TESTS)

cmake_dependent_option(KV_USE_OPENTRACING
                       "Enable support for OpenTracing" ON
                       "OPENTRACING_INCLUDE_DIR;OPENTRACING_LIBRARIES" OFF)
if (KV_USE_OPENTRACING)
    add_definitions(-DENABLE_OPENTRACING)
    message(STATUS "Adding support for OpenTracing")
    message(STATUS "OpenTracing support is currently a proof of concept and should not be used in production")
endif()

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

# Speed up incremental builds by not depending on linked shared
# library files.
#
# Modification to shared library *files* will not be sufficient to
# re-link targets which depend on them; however changes to the shared
# library interface (i.e. headers) will still cause re-linking.
#
# Example: memcached.exe dynamically links to libplatform.so; and
# depends on the various <platform/xxx> header files. With this
# setting, modifications to the implemenation of libplatform.so which
# don't change it's headers will not cause memcached.exe to be
# re-linked.
set(CMAKE_LINK_DEPENDS_NO_SHARED 1)

IF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
    EXECUTE_PROCESS(COMMAND git log -1 --pretty=format:%H
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                    OUTPUT_VARIABLE MEMCACHED_VERSION
                    ERROR_QUIET
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
ENDIF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)

IF ("${MEMCACHED_VERSION}" STREQUAL "")
    string(TIMESTAMP _timestamp UTC)
    set(MEMCACHED_VERSION "unknown:${_timestamp}")
ENDIF ("${MEMCACHED_VERSION}" STREQUAL "")
add_definitions(-DMEMCACHED_VERSION="${MEMCACHED_VERSION}")
add_definitions(-DPRODUCT_VERSION="${PRODUCT_VERSION}")
add_definitions(-DDESTINATION_ROOT="${CMAKE_INSTALL_PREFIX}")
add_definitions(-DSOURCE_ROOT="${Memcached_SOURCE_DIR}")
add_definitions(-DOBJECT_ROOT="${Memcached_BINARY_DIR}")
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DGOOGLE_GLOG_DLL_DECL=)

CHECK_SYMBOL_EXISTS(memalign malloc.h HAVE_MEMALIGN)
if (HAVE_MEMALIGN)
  add_definitions(-DHAVE_MEMALIGN=1)
endif()

if(HAVE_MALLOC_USABLE_SIZE)
  ADD_DEFINITIONS(-DHAVE_MALLOC_USABLE_SIZE)
endif()

if (WIN32)
   # by "default" trying to include <Windows.h> includes a ton of other
   # header files (for instance winsock.h, which conflicts with winsock2.h)
   # In order to work around that we may define WIN32_LEAN_AND_MEAN which
   # cause windows.h to not include all of these extra files.
   # Given that we want this behavior for all of the files in kv_engine
   # we add it as a global flag. Ideally it should have been set in tlm,
   # but that cause forestdb to fail.
   add_definitions(-DWIN32_LEAN_AND_MEAN)

   # 'conversion' conversion from 'type1' to 'type2', possible loss of data
   add_definitions(/wd4244)
   # 'var' : conversion from 'size_t' to 'type', possible loss of data
   add_definitions(/wd4267)
endif()

include_directories(BEFORE
                    ${CMAKE_CURRENT_SOURCE_DIR}/include
                    ${CMAKE_CURRENT_SOURCE_DIR}
                    ${CMAKE_CURRENT_BINARY_DIR}/include)

include_directories(AFTER SYSTEM
                    ${Platform_SOURCE_DIR}/include
                    ${phosphor_SOURCE_DIR}/include
                    ${gsl_lite_SOURCE_DIR}/include
                    ${spdlog_SOURCE_DIR}/include
                    ${gtest_SOURCE_DIR}/include
                    ${gmock_SOURCE_DIR}/include
                    ${subjson_SOURCE_DIR}
                    ${LIBEVENT_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${FOLLY_INCLUDE_DIR}
                    ${BOOST_INCLUDE_DIR})

if (WIN32)
    include_directories(BEFORE SYSTEM ${Platform_SOURCE_DIR}/include/win32)
endif (WIN32)

IF (MEMORY_ALLOCATOR)
    include_directories(AFTER SYSTEM ${MALLOC_INCLUDE_DIR})
ELSE (MEMORY_ALLOCATOR)
    SET(MALLOC_LIBRARIES "")
ENDIF (MEMORY_ALLOCATOR)

IF (BREAKPAD_FOUND)
    ADD_DEFINITIONS(-DHAVE_BREAKPAD)
    IF (UNIX AND COUCHBASE_KV_BUILD_UNIT_TESTS)
        # On Linux Breakpad unit test relies on examining the minidump -> core
        # file with GDB.
        FIND_PROGRAM(GDB_EXECUTABLE gdb)
        IF (GDB_EXECUTABLE)
            # Three tests - crashing via segfault, std::exception and unknown exception
            ADD_TEST(memcached-breakpad-test-segfault ${PYTHON_EXECUTABLE}
                     ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
                     ${Memcached_BINARY_DIR}/memcached segfault ${MINIDUMP2CORE}
                     ${GDB_EXECUTABLE})

            ADD_TEST(memcached-breakpad-test-std-exception ${PYTHON_EXECUTABLE}
                     ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
                     ${Memcached_BINARY_DIR}/memcached std_exception ${MINIDUMP2CORE}
                     ${GDB_EXECUTABLE})

            ADD_TEST(memcached-breakpad-test-unknown-exception ${PYTHON_EXECUTABLE}
                     ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
                     ${Memcached_BINARY_DIR}/memcached unknown_exception ${MINIDUMP2CORE}
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
    ENDIF (UNIX AND COUCHBASE_KV_BUILD_UNIT_TESTS)
ENDIF (BREAKPAD_FOUND)

ADD_SUBDIRECTORY(etc)
ADD_SUBDIRECTORY(auditd)
ADD_SUBDIRECTORY(cbcrypto)
ADD_SUBDIRECTORY(cbsasl)
ADD_SUBDIRECTORY(time)
ADD_SUBDIRECTORY(rbac)
ADD_SUBDIRECTORY(protocol)
ADD_SUBDIRECTORY(utilities)
ADD_SUBDIRECTORY(engines)
ADD_SUBDIRECTORY(xattr)
ADD_SUBDIRECTORY(logger)
ADD_SUBDIRECTORY(daemon)
ADD_SUBDIRECTORY(tracing)
ADD_SUBDIRECTORY(statistics)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp.pem COPYONLY)
    CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp.cert COPYONLY)
    CONFIGURE_FILE(tests/cert/client.pem tests/cert/client.pem COPYONLY)
    CONFIGURE_FILE(tests/cert/client.key tests/cert/client.key COPYONLY)
    CONFIGURE_FILE(tests/cert/parse-test.key tests/cert/parse-test.key COPYONLY)
    CONFIGURE_FILE(tests/cert/parse-test.pem tests/cert/parse-test.pem COPYONLY)
    # Second copy to test changing at runtime
    CONFIGURE_FILE(tests/cert/testapp.pem tests/cert/testapp2.pem COPYONLY)
    CONFIGURE_FILE(tests/cert/testapp.cert tests/cert/testapp2.cert COPYONLY)

    ADD_SUBDIRECTORY(testsuite)
    ADD_SUBDIRECTORY(tests)

    ENABLE_CODE_COVERAGE_REPORT()
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)

add_subdirectory(programs)

# Customize some CTest properties
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/CTestCustom.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/CTestCustom.cmake COPYONLY)

option(CB_ENABLE_HEADER_INCLUDE_CHECK
       "Enable building of just headers objs, for header include correctness.")

if(CB_ENABLE_HEADER_INCLUDE_CHECK)
    # Extra (non-default) target to compile all memcached headers (for
    # build profiling / checking for correct #includes).
    #
    # Note this is all kv_engine headers /apart/ from ep_engine (which
    # requires additional include directories and hence is handled in it's
    # own CMakeLists.txt)
    #
    # TODO: When we upgrade to CMake 3.6+, remove the explicit whitelist
    # and instead GLOB all *.h, then use list(FILTER) to exclude
    # engines/ep - see previous version of this code).
    file(GLOB_RECURSE memcached_headers
      RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
      CONFIGURE_DEPENDS
      auditd/*.h
      cbcrypto/*.h
      cbsasl/*.h
      daemon/*.h
      engines/default/*.h
      engines/crash_engine/*.h
      engines/default_engine.h
      engines/default_engine/*.h
      engines/crash_engine/*.h
      engines/ewouldblock_engine/*.h
      engines/nobucket/*.h
      engines/utilities/*.h
      include/*.h
      logger/*.h
      programs/*.h
      protocol/*.h
      rbac/*.h
      tests/*.h
      testsuite/*.h
      time/*.h
      tracing/*.h
      utilities/*.h
      xattr/*.h
      )

    # List of headers to ignore / not attempt to compile
    list(REMOVE_ITEM memcached_headers
      config.cmake.h  # Not directly compilable.
      include/memcached/collections.h  # Not directly compilable; used via engine.h
    )


    add_header_object_library(NAME memcached_headers_obj HEADERS ${memcached_headers})
    target_include_directories(memcached_headers_obj SYSTEM PRIVATE
      ${hdr_histogram_SOURCE_DIR}/src
      ${OPENTRACING_INCLUDE_DIR})
endif(CB_ENABLE_HEADER_INCLUDE_CHECK)
