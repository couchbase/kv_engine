PROJECT(Memcached LANGUAGES C CXX)

INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckIncludeFiles)
INCLUDE(CheckIncludeFileCXX)
INCLUDE(CTest)
INCLUDE(GenerateExportHeader)
include(HeaderObjectLibrary)

IF(COUCHBASE_KV_COMMIT_VALIDATION AND NOT CB_DEVELOPMENT_ASSERTS)
    message(FATAL_ERROR "CB_DEVELOPMENT_ASSERTS MUST have been enabled for KV_COMMIT_VALIDATION builds to ensure that tests are valid.")
ENDIF(COUCHBASE_KV_COMMIT_VALIDATION AND NOT CB_DEVELOPMENT_ASSERTS)

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

if (WIN32)
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
                    ${phosphor_SOURCE_DIR}/include
                    ${gsl_lite_SOURCE_DIR}/include
                    ${LIBEVENT_INCLUDE_DIR}
                    ${OPENSSL_INCLUDE_DIR}
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
ENDIF (BREAKPAD_FOUND)
IF (UNIX)
    # On Linux Breakpad unit test relies on examining the minidump -> core
    # file with GDB.
    find_program(GDB_EXECUTABLE gdb)
    set(test_extra_args --source_root=${Memcached_SOURCE_DIR})
    if (BREAKPAD_FOUND)
        list(APPEND test_extra_args --breakpad)
    endif ()
    if (MINIDUMP2CORE)
        list(APPEND test_extra_args --md2core_exe=${MINIDUMP2CORE})
    endif()
    if (GDB_EXECUTABLE)
        list(APPEND test_extra_args --gdb_exe=${GDB_EXECUTABLE})
    endif()
    function(add_breakpad_test VARIANT)
        add_test(memcached-breakpad-test-${VARIANT}
             ${Memcached_SOURCE_DIR}/tests/breakpad_test.py
             ${Memcached_BINARY_DIR}/memcached ${VARIANT} ${test_extra_args})
        set_tests_properties(memcached-breakpad-test-${VARIANT}
            PROPERTIES TIMEOUT 60)
    endfunction()

    add_breakpad_test(segfault)
    add_breakpad_test(std_exception)
    add_breakpad_test(std_exception_with_trace)
    add_breakpad_test(std_exception_via_std_thread)
    add_breakpad_test(unknown_exception)

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

# We commonly use a bunch of expensive headers so pre-compiling them should give
# us a speed-up. It's simpler to define a single target and just re-use the
# pre-compiled headers everywhere.
# Some tools such as clang-tidy which only runs on changed files struggles with
# pre-compiled headers. As such, we want to make them optional (but opt-out
# rather than opt-in as they should generally be an improvement). Because of
# this, users of the pre-compiled header target should include it via one of the
# below functions
if (CB_PCH)
    # Given consumers of precompiled headers must have the same compile flags,
    # we need two PCH targets - with and without position-independent code.
    foreach(pch_lib kv_pch kv_pch_fpic)
        # Need to give the target a SOURCE to make cmake happy so just including
        # an empty .cc file.
        add_library(${pch_lib} OBJECT precompiled_headers.cc)
        target_link_libraries(${pch_lib}
                              PUBLIC
                                 Folly::headers
                              PRIVATE
                                 nlohmann_json::nlohmann_json
                                 GTest::gtest)
        # Hmm.. If I remove the target_include_directories I get all sort of
        # other errors. Just point to the project include directory for now..
        target_include_directories(${pch_lib} PUBLIC ${Memcached_SOURCE_DIR}/include)
        target_precompile_headers(${pch_lib} PUBLIC precompiled_headers.h)
    endforeach()
    set_property(TARGET kv_pch_fpic PROPERTY POSITION_INDEPENDENT_CODE 1)
endif()

function(kv_enable_pch target)
    if (CB_PCH)
        # ASan/TSan builds don't like a mix of position independent code so we
        # need to use the correct pch target
        get_property(fpic TARGET ${target} PROPERTY POSITION_INDEPENDENT_CODE)
        if (fpic)
            reuse_pch(${target} kv_pch_fpic)
        else()
            reuse_pch(${target} kv_pch)
        endif()
    endif()
endfunction()

add_subdirectory(json)
ADD_SUBDIRECTORY(etc)
ADD_SUBDIRECTORY(auditd)
ADD_SUBDIRECTORY(cbsasl)
add_subdirectory(executor)
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
add_subdirectory(cluster_framework)

add_subdirectory(testsuite)
add_subdirectory(tests)
enable_code_coverage_report()

add_subdirectory(programs)

# Customize some CTest properties
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/CTestCustom.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/CTestCustom.cmake COPYONLY)

option(CB_ENABLE_HEADER_INCLUDE_CHECK
       "Enable building of just headers objs, for header include correctness.")

if(CB_ENABLE_HEADER_INCLUDE_CHECK)
    # Extra (non-default) target to compile all memcached headers (for
    # build profiling / checking for correct #includes).
    file(GLOB_RECURSE memcached_headers
      RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
      CONFIGURE_DEPENDS
      *.h)
    # Excluded ep_engine - it requires additional include directories and
    # hence is handled in it's own CMakeLists.txt
    list(FILTER memcached_headers EXCLUDE REGEX "engines\/ep\/")

    # List of headers to ignore / not attempt to compile
    list(REMOVE_ITEM memcached_headers
      config.cmake.h  # Not directly compilable.
      include/memcached/collections.h  # Not directly compilable; used via engine.h
    )

    add_header_object_library(NAME memcached_headers_obj HEADERS ${memcached_headers})
    target_include_directories(memcached_headers_obj SYSTEM PRIVATE
      ${OPENTRACING_INCLUDE_DIR})
endif(CB_ENABLE_HEADER_INCLUDE_CHECK)

add_custom_target(generated_source_files
        DEPENDS
        ep_generated_source_files
        generated_stats_source_files
        )
