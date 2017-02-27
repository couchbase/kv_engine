PROJECT(EventuallyPersistentEngine)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

IF (${CMAKE_MAJOR_VERSION} GREATER 2)
    CMAKE_POLICY(SET CMP0042 NEW)
ENDIF (${CMAKE_MAJOR_VERSION} GREATER 2)

INCLUDE(CheckFunctionExists)
INCLUDE(CheckIncludeFileCXX)
INCLUDE(CheckIncludeFiles)
INCLUDE(CheckLibraryExists)
INCLUDE(CheckSymbolExists)
INCLUDE(CheckTypeSize)
INCLUDE(CTest)

OPTION(EP_USE_FORESTDB "Enable support for ForestDB" OFF)

INCLUDE_DIRECTORIES(BEFORE ${CMAKE_INSTALL_PREFIX}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/src
                           ${CMAKE_CURRENT_BINARY_DIR}/src
                           ${SNAPPY_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${Platform_SOURCE_DIR}/external
                           ${Memcached_SOURCE_DIR}
                           ${Memcached_SOURCE_DIR}/utilities
                           ${Memcached_SOURCE_DIR}/include
                           ${Couchstore_SOURCE_DIR}/include
                           ${phosphor_SOURCE_DIR}/include
                           ${CMAKE_CURRENT_BINARY_DIR})

INCLUDE_DIRECTORIES(AFTER ${Memcached_BINARY_DIR}/include)

IF (EP_USE_FORESTDB)
    INCLUDE_DIRECTORIES(AFTER ${ForestDB_SOURCE_DIR}/include)
    SET(FOREST_KVSTORE_SOURCE src/forest-kvstore/forest-kvstore.cc)
    SET(EP_FORESTDB_LIB forestdb)
    ADD_DEFINITIONS(-DEP_USE_FORESTDB=1)
    MESSAGE(STATUS "ep-engine: Using ForestDB")
ENDIF (EP_USE_FORESTDB)

INCLUDE_DIRECTORIES(AFTER
                    ${gtest_SOURCE_DIR}/include
                    ${gmock_SOURCE_DIR}/include)

CHECK_INCLUDE_FILES("alloca.h" HAVE_ALLOCA_H)
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("unistd.h" HAVE_UNISTD_H)
CHECK_INCLUDE_FILES("netdb.h" HAVE_NETDB_H)
CHECK_INCLUDE_FILES("mach/mach_time.h" HAVE_MACH_MACH_TIME_H)
CHECK_INCLUDE_FILES("poll.h" HAVE_POLL_H)
CHECK_INCLUDE_FILES("sysexits.h" HAVE_SYSEXITS_H)
CHECK_INCLUDE_FILES("unistd.h" HAVE_UNISTD_H)
CHECK_INCLUDE_FILES("sched.h" HAVE_SCHED_H)
CHECK_INCLUDE_FILES("sys/socket.h" HAVE_SYS_SOCKET_H)
CHECK_INCLUDE_FILES("sys/time.h" HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILES("netinet/in.h" HAVE_NETINET_IN_H)
CHECK_INCLUDE_FILES("netinet/tcp.h" HAVE_NETINET_TCP_H)
CHECK_INCLUDE_FILE_CXX("unordered_map" HAVE_UNORDERED_MAP)
CHECK_INCLUDE_FILE_CXX("atomic" HAVE_ATOMIC)
CHECK_INCLUDE_FILE_CXX("thread" HAVE_THREAD)

CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
CHECK_FUNCTION_EXISTS(mach_absolute_time HAVE_MACH_ABSOLUTE_TIME)
CHECK_FUNCTION_EXISTS(gettimeofday HAVE_GETTIMEOFDAY)
CHECK_FUNCTION_EXISTS(getopt_long HAVE_GETOPT_LONG)

# For debugging without compiler optimizations uncomment line below..
#SET (CMAKE_BUILD_TYPE DEBUG)

IF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
   EXECUTE_PROCESS(COMMAND git rev-parse HEAD
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                   OUTPUT_VARIABLE EP_ENGINE_VERSION
                   OUTPUT_STRIP_TRAILING_WHITESPACE)
   IF (EP_ENGINE_VERSION STREQUAL "" OR EP_ENGINE_VERSION STREQUAL "HEAD")
     MESSAGE (FATAL_ERROR "Failed to determine commit SHA!")
   ENDIF (EP_ENGINE_VERSION STREQUAL "" OR EP_ENGINE_VERSION STREQUAL "HEAD")
ELSE (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
   SET(EP_ENGINE_VERSION "unknown")
ENDIF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)

CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/src/config.cmake.h
                ${CMAKE_CURRENT_BINARY_DIR}/src/config.h)

# Generate the python wrappers
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbepctl)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbstats)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbcompact)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvdiff)
CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/wrapper/wrapper
                ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvbucketctl)

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${CMAKE_SOURCE_DIR}/platform/include/win32)
ENDIF (WIN32)

ADD_EXECUTABLE(gencode tools/gencode.cc)
TARGET_LINK_LIBRARIES(gencode cJSON platform)

ADD_EXECUTABLE(genconfig tools/genconfig.cc)
TARGET_LINK_LIBRARIES(genconfig cJSON platform)

ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
                          ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h
                  COMMAND
                     gencode -j ${CMAKE_CURRENT_SOURCE_DIR}/docs/stats.json -h ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h -c ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c -f get_stats_info
                  DEPENDS
                        docs/stats.json
                        gencode
                  COMMENT "Generating code for stats info")

ADD_CUSTOM_COMMAND(OUTPUT
                     ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.cc
                     ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.h
                  COMMAND
                     genconfig ${CMAKE_CURRENT_SOURCE_DIR}/configuration.json
                  DEPENDS
                        configuration.json
                        genconfig
                  COMMENT "Generating code for configuration class")

SET(KVSTORE_SOURCE src/kvstore.cc)
SET(COUCH_KVSTORE_SOURCE src/couch-kvstore/couch-kvstore.cc
            src/couch-kvstore/couch-fs-stats.cc)
SET(OBJECTREGISTRY_SOURCE src/objectregistry.cc)
SET(CONFIG_SOURCE src/configuration.cc
  ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.cc)

SET(COLLECTIONS_SOURCE src/collections/manifest.cc
                       src/collections/vbucket_manifest.cc
                       src/collections/vbucket_manifest_entry.cc)

ADD_LIBRARY(ep_objs OBJECT
            src/access_scanner.cc
            src/atomic.cc
            src/backfill.cc
            src/bgfetcher.cc
            src/bloomfilter.cc
            src/checkpoint.cc
            src/checkpoint_remover.cc
            src/conflict_resolution.cc
            src/connmap.cc
            src/crc32.c
            src/dcp/backfill-manager.cc
            src/dcp/backfill.cc
            src/dcp/consumer.cc
            src/dcp/dcpconnmap.cc
            src/dcp/flow-control.cc
            src/dcp/flow-control-manager.cc
            src/dcp/producer.cc
            src/dcp/response.cc
            src/dcp/stream.cc
            src/defragmenter.cc
            src/defragmenter_visitor.cc
            src/ep_bucket.cc
            src/ep_vb.cc
            src/ep_engine.cc
            src/ep_time.cc
            src/ephemeral_bucket.cc
            src/ephemeral_vb.cc
            src/executorpool.cc
            src/executorthread.cc
            src/ext_meta_parser.cc
            src/failover-table.cc
            src/flusher.cc
            src/globaltask.cc
            src/hash_table.cc
            src/htresizer.cc
            src/item.cc
            src/item_pager.cc
            src/logger.cc
            src/kv_bucket.cc
            src/kvshard.cc
            src/memory_tracker.cc
            src/murmurhash3.cc
            src/mutation_log.cc
            src/mutation_log_entry.cc
            src/pre_link_document_context.cc
            src/pre_link_document_context.h
            src/replicationthrottle.cc
            src/string_utils.cc
            src/storeddockey.cc
            src/stored-value.cc
            src/systemevent.cc
            src/tapconnection.cc
            src/tapconnmap.cc
            src/tasks.cc
            src/taskqueue.cc
            src/vbucket.cc
            src/vbucketmap.cc
            src/vbucketmemorydeletiontask.cc
            src/warmup.cc
            ${OBJECTREGISTRY_SOURCE}
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
            ${CONFIG_SOURCE}
            ${KVSTORE_SOURCE}
            ${COUCH_KVSTORE_SOURCE}
            ${FOREST_KVSTORE_SOURCE}
            ${COLLECTIONS_SOURCE})
SET_PROPERTY(TARGET ep_objs PROPERTY POSITION_INDEPENDENT_CODE 1)

ADD_LIBRARY(ep SHARED $<TARGET_OBJECTS:ep_objs>)

SET_TARGET_PROPERTIES(ep PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep cJSON JSON_checker couchstore ${EP_FORESTDB_LIB}
                      engine_utilities dirutils cbcompress
                      platform phosphor ${LIBEVENT_LIBRARIES})

# Single executable containing all class-level unit tests involving
# EventuallyPersistentEngine driven by GoogleTest.
# (We end up compiling most of the src/ files of ep-engine for these unit tests,
# so simpler / quicker just to link them into a single executable).
ADD_EXECUTABLE(ep-engine_ep_unit_tests
               tests/mock/mock_dcp.cc
               tests/mock/mock_synchronous_ep_engine.cc
               tests/module_tests/atomic_unordered_map_test.cc
               tests/module_tests/bloomfilter_test.cc
               tests/module_tests/checkpoint_test.cc
               tests/module_tests/collections/collection_dockey_test.cc
               tests/module_tests/collections/manifest_test.cc
               tests/module_tests/collections/vbucket_manifest_test.cc
               tests/module_tests/collections/vbucket_manifest_entry_test.cc
               tests/module_tests/configuration_test.cc
               tests/module_tests/defragmenter_test.cc
               tests/module_tests/dcp_test.cc
               tests/module_tests/ep_unit_tests_main.cc
               tests/module_tests/evp_engine_test.cc
               tests/module_tests/evp_store_rollback_test.cc
               tests/module_tests/evp_store_test.cc
               tests/module_tests/evp_store_single_threaded_test.cc
               tests/module_tests/executorpool_test.cc
               tests/module_tests/failover_table_test.cc
               tests/module_tests/futurequeue_test.cc
               tests/module_tests/hash_table_test.cc
               tests/module_tests/kvstore_test.cc
               tests/module_tests/memory_tracker_test.cc
               tests/module_tests/mock_hooks_api.cc
               tests/module_tests/mutation_log_test.cc
               tests/module_tests/mutex_test.cc
               tests/module_tests/stats_test.cc
               tests/module_tests/storeddockey_test.cc
               tests/module_tests/systemevent_test.cc
               tests/module_tests/vbucket_test.cc
               tests/module_tests/warmup_test.cc
               $<TARGET_OBJECTS:ep_objs>
               $<TARGET_OBJECTS:memory_tracking>
               $<TARGET_OBJECTS:couchstore_test_fileops>
               ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc
               ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
               ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.cc
               ${Memcached_SOURCE_DIR}/utilities/string_utilities.cc)

TARGET_INCLUDE_DIRECTORIES(ep-engine_ep_unit_tests
        PUBLIC
        ${Couchstore_SOURCE_DIR})

TARGET_LINK_LIBRARIES(ep-engine_ep_unit_tests couchstore cJSON dirutils
                      engine_utilities ${EP_FORESTDB_LIB}
                      gtest gmock JSON_checker mcd_util platform
                      phosphor xattr cbcompress ${MALLOC_LIBRARIES})

ADD_EXECUTABLE(ep-engine_atomic_ptr_test
  tests/module_tests/atomic_ptr_test.cc
  src/atomic.cc
  src/testlogger.cc)
TARGET_LINK_LIBRARIES(ep-engine_atomic_ptr_test platform)

ADD_EXECUTABLE(ep-engine_couch-fs-stats_test
        src/couch-kvstore/couch-fs-stats.cc
        src/generated_configuration.h
        tests/module_tests/couch-fs-stats_test.cc
        $<TARGET_OBJECTS:couchstore_wrapped_fileops_test_framework>)
TARGET_INCLUDE_DIRECTORIES(ep-engine_couch-fs-stats_test
        PRIVATE
        ${Couchstore_SOURCE_DIR}
        ${Couchstore_SOURCE_DIR}/src)
TARGET_LINK_LIBRARIES(ep-engine_couch-fs-stats_test gtest gtest_main gmock platform)

ADD_EXECUTABLE(ep-engine_hrtime_test tests/module_tests/hrtime_test.cc)
TARGET_LINK_LIBRARIES(ep-engine_hrtime_test platform)

ADD_EXECUTABLE(ep-engine_misc_test tests/module_tests/misc_test.cc)
TARGET_LINK_LIBRARIES(ep-engine_misc_test platform)

ADD_EXECUTABLE(ep-engine_string_utils_test
               tests/module_tests/string_utils_test.cc
               src/string_utils.cc)
TARGET_LINK_LIBRARIES(ep-engine_string_utils_test gtest gtest_main platform)

ADD_TEST(ep-engine_atomic_ptr_test ep-engine_atomic_ptr_test)
ADD_TEST(ep-engine_couch-fs-stats_test ep-engine_couch-fs-stats_test)
ADD_TEST(ep-engine_ep_unit_tests ep-engine_ep_unit_tests)
ADD_TEST(ep-engine_hrtime_test ep-engine_hrtime_test)
ADD_TEST(ep-engine_misc_test ep-engine_misc_test)

ADD_LIBRARY(timing_tests SHARED tests/module_tests/timing_tests.cc)
SET_TARGET_PROPERTIES(timing_tests PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(timing_tests platform)

ADD_EXECUTABLE(ep-engine_sizes src/sizes.cc
                               $<TARGET_OBJECTS:ep_objs>)
TARGET_LINK_LIBRARIES(ep-engine_sizes cJSON JSON_checker
  engine_utilities couchstore
  ${EP_FORESTDB_LIB} dirutils cbcompress platform phosphor
  ${LIBEVENT_LIBRARIES})

ADD_LIBRARY(ep_testsuite SHARED
   tests/ep_testsuite.cc
   src/ext_meta_parser.cc
   tests/ep_testsuite_common.cc
   tests/ep_test_apis.cc
   tests/mock/mock_dcp.cc
   ${Memcached_SOURCE_DIR}/utilities/string_utilities.cc)
SET_TARGET_PROPERTIES(ep_testsuite PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite couchstore dirutils JSON_checker platform
                      xattr
                      ${LIBEVENT_LIBRARIES})
ADD_DEPENDENCIES(ep_testsuite engine_testapp)

ADD_LIBRARY(ep_testsuite_basic SHARED
   tests/ep_testsuite_basic.cc
   src/ext_meta_parser.cc
   tests/ep_testsuite_common.cc
   tests/ep_test_apis.cc
   tests/mock/mock_dcp.cc
)
SET_TARGET_PROPERTIES(ep_testsuite_basic PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite_basic JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})
ADD_DEPENDENCIES(ep_testsuite engine_testapp)

ADD_LIBRARY(ep_testsuite_dcp SHARED
tests/ep_testsuite_dcp.cc
src/ext_meta_parser.cc
tests/ep_testsuite_common.cc
tests/ep_test_apis.cc
tests/mock/mock_dcp.cc
)
SET_TARGET_PROPERTIES(ep_testsuite_dcp PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite_dcp cbcompress JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})
ADD_DEPENDENCIES(ep_testsuite_dcp engine_testapp)

ADD_LIBRARY(ep_testsuite_tap SHARED
   tests/ep_testsuite_common.cc
   tests/ep_testsuite_tap.cc
   tests/ep_test_apis.cc
   src/ext_meta_parser.cc)
SET_TARGET_PROPERTIES(ep_testsuite_tap PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite_tap JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})
ADD_DEPENDENCIES(ep_testsuite_tap engine_testapp)

ADD_LIBRARY(ep_testsuite_checkpoint SHARED
   tests/ep_testsuite_common.cc
   tests/ep_testsuite_checkpoint.cc
   tests/ep_test_apis.cc
   src/ext_meta_parser.cc)
SET_TARGET_PROPERTIES(ep_testsuite_checkpoint PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite_checkpoint JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})
ADD_DEPENDENCIES(ep_testsuite_checkpoint engine_testapp)

ADD_LIBRARY(ep_testsuite_xdcr SHARED
   tests/ep_testsuite_common.cc
   tests/ep_testsuite_xdcr.cc
   tests/ep_test_apis.cc
   src/ext_meta_parser.cc)
SET_TARGET_PROPERTIES(ep_testsuite_xdcr PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite_xdcr JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})
ADD_DEPENDENCIES(ep_testsuite_xdcr engine_testapp)

ADD_LIBRARY(ep_perfsuite SHARED
   tests/ep_perfsuite.cc
   src/ext_meta_parser.cc
   tests/ep_testsuite_common.cc
   tests/ep_test_apis.cc
   tests/mock/mock_dcp.cc)
SET_TARGET_PROPERTIES(ep_perfsuite PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_perfsuite dirutils platform)
ADD_DEPENDENCIES(ep_perfsuite engine_testapp)

#ADD_CUSTOM_COMMAND(OUTPUT
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_0.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_1.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_2.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_3.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_4.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_5.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_6.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_7.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_8.c
#                     ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_9.c
#                  COMMAND
#                     ${PYTHON_EXECUTABLE} tests/module_tests/gen_engine_test.py
#                  DEPENDS
#                        tests/module_tests/gen_engine_test.py
#                        tests/module_tests/breakdancer.py
#                  COMMENT "Generating testsuite")
#
#ADD_LIBRARY(generated_testsuite SHARED
#            tests/suite_stubs.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_0.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_1.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_2.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_3.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_4.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_5.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_6.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_7.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_8.c
#            ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_9.c)

#SET_TARGET_PROPERTIES(generated_testsuite PROPERTIES PREFIX "")

INSTALL(PROGRAMS
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbepctl
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbstats
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbcompact
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvdiff
        ${CMAKE_CURRENT_BINARY_DIR}/wrapper/cbvbucketctl
        management/cbanalyze-core
        DESTINATION bin)

INSTALL(PROGRAMS
        management/cbepctl
        management/cbstats
        management/cbcompact
        management/cbvdiff
        management/cbvbucketctl
        DESTINATION lib/python)

INSTALL(FILES
        management/clitool.py
        management/mc_bin_client.py
        management/mc_bin_server.py
        management/memcacheConstants.py
        management/tap.py
        management/tap_example.py
        DESTINATION lib/python)

INSTALL(FILES
        docs/stats.org
        DESTINATION share/doc/ep-engine)

INSTALL(TARGETS ep
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)


# Defines a testsuite which runs in full and value eviction
# (persistent), ephemeral buckets (which has no eviction).
FUNCTION(ADD_TESTSUITE name timeout)
  CMAKE_PARSE_ARGUMENTS("arg" "SKIP_EPHEMERAL" "" "" ${ARGN} )

  SET(_cmdline
    ${CMAKE_BINARY_DIR}/memcached/engine_testapp
        -E ${CMAKE_CURRENT_BINARY_DIR}/ep.so
        -T ${CMAKE_CURRENT_BINARY_DIR}/${name}.so)
  ADD_TEST(NAME ${name}.value_eviction
         COMMAND ${_cmdline} -v -e "dbname=./${name}.value_eviction.db")
  ADD_TEST(NAME ${name}.full_eviction
         COMMAND ${_cmdline} -v -e "item_eviction_policy=full_eviction$<SEMICOLON>dbname=./${name}.full_eviction.db")
  SET_TESTS_PROPERTIES(${name}.value_eviction PROPERTIES TIMEOUT ${timeout})
  SET_TESTS_PROPERTIES(${name}.full_eviction PROPERTIES TIMEOUT ${timeout})

  IF(NOT arg_SKIP_EPHEMERAL)
    ADD_TEST(NAME ${name}.ephemeral
             COMMAND ${_cmdline} -v -e "bucket_type=ephemeral$<SEMICOLON>dbname=./${name}.ephemeral.db")
    SET_TESTS_PROPERTIES(${name}.ephemeral PROPERTIES TIMEOUT ${timeout})
  ENDIF()
ENDFUNCTION()

ADD_TESTSUITE(ep_testsuite 1800)

ADD_TESTSUITE(ep_testsuite_basic 600)

ADD_TESTSUITE(ep_testsuite_dcp 1200)

# TAP not supported for Ephemeral buckets.
ADD_TESTSUITE(ep_testsuite_tap 180 SKIP_EPHEMERAL)

ADD_TESTSUITE(ep_testsuite_checkpoint 120)

ADD_TESTSUITE(ep_testsuite_xdcr 120)

# ================================ PERF_TESTS ================================ #
SET(_ep_perfsuite_cmdline ${CMAKE_BINARY_DIR}/memcached/engine_testapp -E ${CMAKE_CURRENT_BINARY_DIR}/ep.so -T ${CMAKE_CURRENT_BINARY_DIR}/ep_perfsuite.so -v)

# Micro perf tests: value-only eviction mode. Note that it makes no sense to run
# these tests in full-eviction mode as we disable persistence - see MB-19504.
ADD_CUSTOM_TARGET(test-perfsuite
                  COMMAND ${_ep_perfsuite_cmdline}
                  DEPENDS ${CMAKE_BINARY_DIR}/memcached/engine_testapp
                          ep
                          ep_perfsuite
                  VERBATIM)
ADD_TEST(NAME ep_perfsuite.value_eviction
         COMMAND ${_ep_perfsuite_cmdline} -e "dbname=./ep_perfsuite.value_eviction.db")

ADD_TEST(NAME ep_perfsuite.ephemeral
         COMMAND ${_ep_perfsuite_cmdline} -e "bucket_type=ephemeral$<SEMICOLON>dbname=./ep_perfsuite.ephemeral.db")

# ============================================================================ #

ENABLE_CODE_COVERAGE_REPORT()
