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

INCLUDE_DIRECTORIES(BEFORE ${CMAKE_INSTALL_PREFIX}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/src
                           ${CMAKE_CURRENT_BINARY_DIR}/src
                           ${SNAPPY_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${Memcached_SOURCE_DIR}/include
                           ${Couchstore_SOURCE_DIR}/include
                           ${ForestDB_SOURCE_DIR}/include
                           ${CMAKE_CURRENT_BINARY_DIR})

INCLUDE_DIRECTORIES(AFTER
                    ${gtest_SOURCE_DIR}/include)

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

# ---- uncomment the lines below ONLY for dev/debugging ---
#if ("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
#elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
#endif()

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

SET(KVSTORE_SOURCE src/crc32.c src/kvstore.cc)
SET(COUCH_KVSTORE_SOURCE src/couch-kvstore/couch-kvstore.cc
            src/couch-kvstore/couch-fs-stats.cc)
SET(FOREST_KVSTORE_SOURCE src/forest-kvstore/forest-kvstore.cc)
SET(OBJECTREGISTRY_SOURCE src/objectregistry.cc)
SET(CONFIG_SOURCE src/configuration.cc
  ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.cc)

ADD_LIBRARY(ep SHARED
            src/access_scanner.cc
            src/atomic.cc
            src/backfill.cc
            src/bgfetcher.cc
            src/bloomfilter.cc
            src/checkpoint.cc
            src/checkpoint_remover.cc
            src/compress.cc
            src/conflict_resolution.cc
            src/connmap.cc
            src/dcp/backfill-manager.cc
            src/dcp/backfill.cc
            src/dcp/consumer.cc
            src/dcp/flow-control.cc
            src/dcp/flow-control-manager.cc
            src/dcp/producer.cc
            src/dcp/response.cc
            src/dcp/stream.cc
            src/defragmenter.cc
            src/defragmenter_visitor.cc
            src/ep.cc
            src/ep_engine.cc
            src/ep_time.c
            src/executorpool.cc
            src/executorthread.cc
            src/ext_meta_parser.cc
            src/failover-table.cc
            src/flusher.cc
            src/htresizer.cc
            src/item.cc
            src/item_pager.cc
            src/kvshard.cc
            src/memory_tracker.cc
            src/murmurhash3.cc
            src/mutation_log.cc
            src/priority.cc
            src/replicationthrottle.cc
            src/sizes.cc
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
            src/string_utils.cc
            src/stored-value.cc
            src/tapconnection.cc
            src/tasks.cc
            src/taskqueue.cc
            src/vbucket.cc
            src/vbucketmap.cc
            src/warmup.cc
            ${KVSTORE_SOURCE} ${COUCH_KVSTORE_SOURCE}
            ${FOREST_KVSTORE_SOURCE} ${OBJECTREGISTRY_SOURCE}
            ${CONFIG_SOURCE})

SET_TARGET_PROPERTIES(ep PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep cJSON JSON_checker couchstore forestdb
  dirutils platform ${LIBEVENT_LIBRARIES})

ADD_EXECUTABLE(ep-engine_atomic_ptr_test
  tests/module_tests/atomic_ptr_test.cc
  src/atomic.cc
  src/testlogger.cc)
TARGET_LINK_LIBRARIES(ep-engine_atomic_ptr_test platform)

ADD_EXECUTABLE(ep-engine_atomic_test
  tests/module_tests/atomic_test.cc
  src/testlogger.cc)
TARGET_LINK_LIBRARIES(ep-engine_atomic_test platform)

ADD_EXECUTABLE(ep-engine_checkpoint_test
  tests/module_tests/checkpoint_test.cc
  src/atomic.cc
  src/bloomfilter.cc
  src/checkpoint.cc
  src/compress.cc
  src/failover-table.cc
  src/item.cc
  src/murmurhash3.cc
  src/stored-value.cc
  src/testlogger.cc
  src/vbucket.cc
  tests/module_tests/test_memory_tracker.cc
  ${OBJECTREGISTRY_SOURCE} ${CONFIG_SOURCE})
TARGET_LINK_LIBRARIES(ep-engine_checkpoint_test gtest ${SNAPPY_LIBRARIES} cJSON platform)

ADD_EXECUTABLE(ep-engine_chunk_creation_test
  tests/module_tests/chunk_creation_test.cc)
TARGET_LINK_LIBRARIES(ep-engine_chunk_creation_test platform)

ADD_EXECUTABLE(ep-engine_compress_test
  tests/module_tests/compress_test.cc
  src/compress.cc)
TARGET_LINK_LIBRARIES(ep-engine_compress_test ${SNAPPY_LIBRARIES} platform)

ADD_EXECUTABLE(ep-engine_configuration_test
        tests/module_tests/configuration_test.cc
        src/configuration.cc
        src/generated_configuration.h
        src/objectregistry.cc
        src/testlogger.cc)
TARGET_LINK_LIBRARIES(ep-engine_configuration_test gtest gtest_main platform)

ADD_EXECUTABLE(ep-engine_hash_table_test
  tests/module_tests/hash_table_test.cc
  src/atomic.cc
  src/compress.cc
  src/item.cc
  src/stored-value.cc
  src/testlogger.cc
  tests/module_tests/test_memory_tracker.cc
  ${OBJECTREGISTRY_SOURCE} ${CONFIG_SOURCE})
TARGET_LINK_LIBRARIES(ep-engine_hash_table_test gtest ${SNAPPY_LIBRARIES} platform)

ADD_EXECUTABLE(ep-engine_hrtime_test tests/module_tests/hrtime_test.cc)
TARGET_LINK_LIBRARIES(ep-engine_hrtime_test platform)

ADD_EXECUTABLE(ep-engine_misc_test tests/module_tests/misc_test.cc)
TARGET_LINK_LIBRARIES(ep-engine_misc_test platform)
ADD_EXECUTABLE(ep-engine_mutex_test
               tests/module_tests/mutex_test.cc
               src/testlogger.cc)
TARGET_LINK_LIBRARIES(ep-engine_mutex_test gtest gtest_main platform)

ADD_EXECUTABLE(ep-engine_priority_test  tests/module_tests/priority_test.cc src/priority.cc)
TARGET_LINK_LIBRARIES(ep-engine_priority_test platform)

ADD_EXECUTABLE(ep-engine_ringbuffer_test tests/module_tests/ringbuffer_test.cc)
TARGET_LINK_LIBRARIES(ep-engine_ringbuffer_test platform)

ADD_EXECUTABLE(ep-engine_string_utils_test
               tests/module_tests/string_utils_test.cc
               src/string_utils.cc)
TARGET_LINK_LIBRARIES(ep-engine_string_utils_test gtest gtest_main platform)

ADD_EXECUTABLE(ep-engine_failover_table_test tests/module_tests/failover_table_test.cc
                        src/failover-table.cc src/testlogger.cc
                        tests/module_tests/test_memory_tracker.cc
                        ${OBJECTREGISTRY_SOURCE} ${CONFIG_SOURCE})
TARGET_LINK_LIBRARIES(ep-engine_failover_table_test cJSON platform)

ADD_EXECUTABLE(ep-engine_kvstore_test
  tests/module_tests/kvstore_test.cc
  src/compress.cc
  src/testlogger.cc
  ${OBJECTREGISTRY_SOURCE} ${KVSTORE_SOURCE} ${COUCH_KVSTORE_SOURCE}
  ${FOREST_KVSTORE_SOURCE} ${CONFIG_SOURCE})
TARGET_LINK_LIBRARIES(ep-engine_kvstore_test cJSON JSON_checker couchstore
  forestdb dirutils platform)

ADD_TEST(ep-engine_atomic_ptr_test ep-engine_atomic_ptr_test)
ADD_TEST(ep-engine_atomic_test ep-engine_atomic_test)
ADD_TEST(ep-engine_checkpoint_test ep-engine_checkpoint_test)
ADD_TEST(ep-engine_chunk_creation_test ep-engine_chunk_creation_test)
ADD_TEST(ep-engine_compress_test ep-engine_compress_test)
ADD_TEST(ep-engine_configuration_test ep-engine_configuration_test)
ADD_TEST(ep-engine_failover_table_test ep-engine_failover_table_test)
ADD_TEST(ep-engine_hash_table_test ep-engine_hash_table_test)
ADD_TEST(ep-engine_hrtime_test ep-engine_hrtime_test)
ADD_TEST(ep-engine_misc_test ep-engine_misc_test)
ADD_TEST(ep-engine_mutex_test ep-engine_mutex_test)
ADD_TEST(ep-engine_priority_test ep-engine_priority_test)
ADD_TEST(ep-engine_ringbuffer_test ep-engine_ringbuffer_test)
ADD_TEST(ep-engine_kvstore_test ep-engine_kvstore_test)

ADD_LIBRARY(timing_tests SHARED tests/module_tests/timing_tests.cc)
SET_TARGET_PROPERTIES(timing_tests PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(timing_tests platform)

ADD_EXECUTABLE(ep-engine_sizes src/sizes.cc src/mutex.h src/testlogger.cc
              ${OBJECTREGISTRY_SOURCE} ${CONFIG_SOURCE})
TARGET_LINK_LIBRARIES(ep-engine_sizes platform)

ADD_EXECUTABLE(ep-engine_defragmenter_test
               tests/module_tests/defragmenter_test.cc
               src/bloomfilter.cc
               src/checkpoint.cc
               src/compress.cc
               src/configuration.cc
               src/defragmenter_visitor.cc
               src/ep_time.c
               src/generated_configuration.cc
               src/failover-table.cc
               src/item.cc
               src/murmurhash3.cc
               src/stored-value.cc
               src/testlogger.cc
               src/vbucket.cc
               ${OBJECTREGISTRY_SOURCE})
TARGET_LINK_LIBRARIES(ep-engine_defragmenter_test cJSON platform ${SNAPPY_LIBRARIES})

ADD_LIBRARY(ep_testsuite SHARED
   tests/ep_testsuite.cc
   src/atomic.cc
   src/compress.cc
   src/ep_time.c
   src/ext_meta_parser.cc
   src/item.cc
   src/testlogger.cc
   tests/ep_testsuite_common.cc
   tests/ep_test_apis.cc
   tests/mock/mock_dcp.cc
   ${OBJECTREGISTRY_SOURCE}
   ${CONFIG_SOURCE})
SET_TARGET_PROPERTIES(ep_testsuite PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})

ADD_LIBRARY(ep_testsuite_basic SHARED
   tests/ep_testsuite_basic.cc
   src/compress.cc
   src/ext_meta_parser.cc
   tests/ep_testsuite_common.cc
   tests/ep_test_apis.cc
   tests/mock/mock_dcp.cc
)
SET_TARGET_PROPERTIES(ep_testsuite_basic PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite_basic JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})

ADD_LIBRARY(ep_testsuite_dcp SHARED
tests/ep_testsuite_dcp.cc
src/compress.cc
src/ext_meta_parser.cc
tests/ep_testsuite_common.cc
tests/ep_test_apis.cc
tests/mock/mock_dcp.cc
)
SET_TARGET_PROPERTIES(ep_testsuite_dcp PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_testsuite_dcp JSON_checker dirutils platform ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})

ADD_LIBRARY(ep_perfsuite SHARED
   tests/ep_perfsuite.cc
   src/ext_meta_parser.cc
   tests/ep_testsuite_common.cc
   tests/ep_test_apis.cc
   tests/mock/mock_dcp.cc)
SET_TARGET_PROPERTIES(ep_perfsuite PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(ep_perfsuite dirutils platform)

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

# =============================== ENGINE_TESTS =============================== #
SET(_ep_testsuite_cmdline ${CMAKE_BINARY_DIR}/memcached/engine_testapp -E ${CMAKE_CURRENT_BINARY_DIR}/ep.so -T ${CMAKE_CURRENT_BINARY_DIR}/ep_testsuite.so)

# Engine tests: value-only eviction mode
ADD_TEST(NAME ep-engine_engine_tests
         COMMAND ${_ep_testsuite_cmdline} -v -e "dbname=./value_eviction_test")
SET_TESTS_PROPERTIES(ep-engine_engine_tests PROPERTIES TIMEOUT 1800)

# Engine tests: full eviction mode
ADD_TEST(NAME ep-engine_full_eviction_tests
         COMMAND ${_ep_testsuite_cmdline} -v -e "item_eviction_policy=full_eviction;dbname=./full_eviction_test")
SET_TESTS_PROPERTIES(ep-engine_full_eviction_tests PROPERTIES TIMEOUT 1800)

# ============================================================================ #

# =============================== BASIC_TESTS ================================ #
SET(_ep_testsuite_basic_cmdline ${CMAKE_BINARY_DIR}/memcached/engine_testapp -E ${CMAKE_CURRENT_BINARY_DIR}/ep.so -T ${CMAKE_CURRENT_BINARY_DIR}/ep_testsuite_basic.so)

# Basic engine tests: value-only eviction mode
ADD_TEST(NAME ep-engine_basic_tests
         COMMAND ${_ep_testsuite_basic_cmdline} -v -e "dbname=./value_eviction_basic")
SET_TESTS_PROPERTIES(ep-engine_basic_tests PROPERTIES TIMEOUT 400)

ADD_TEST(NAME ep-engine_full_eviction_basic
         COMMAND ${_ep_testsuite_basic_cmdline} -v -e "item_eviction_policy=full_eviction;dbname=./full_eviction_basic")
SET_TESTS_PROPERTIES(ep-engine_full_eviction_basic PROPERTIES TIMEOUT 400)

# ============================================================================ #

# ================================ DCP_TESTS ================================= #
SET(_ep_testsuite_dcp_cmdline ${CMAKE_BINARY_DIR}/memcached/engine_testapp -E ${CMAKE_CURRENT_BINARY_DIR}/ep.so -T ${CMAKE_CURRENT_BINARY_DIR}/ep_testsuite_dcp.so)

# DCP engine tests: value-only eviction mode
ADD_TEST(NAME ep-engine_dcp_tests
         COMMAND ${_ep_testsuite_dcp_cmdline} -v -e "dbname=./value_eviction_dcp")
SET_TESTS_PROPERTIES(ep-engine_dcp_tests PROPERTIES TIMEOUT 1800)

# DCP engine tests: value-only eviction mode
ADD_TEST(NAME ep-engine_full_eviction_dcp
         COMMAND ${_ep_testsuite_dcp_cmdline} -v -e "item_eviction_policy=full_eviction;dbname=./full_eviction_dcp")
SET_TESTS_PROPERTIES(ep-engine_full_eviction_dcp PROPERTIES TIMEOUT 1800)

# ============================================================================ #

# ================================ PERF_TESTS ================================ #
SET(_ep_perfsuite_cmdline ${CMAKE_BINARY_DIR}/memcached/engine_testapp -E ${CMAKE_CURRENT_BINARY_DIR}/ep.so -T ${CMAKE_CURRENT_BINARY_DIR}/ep_perfsuite.so -v)

# Micro perf tests: value-only eviction mode
ADD_CUSTOM_TARGET(test-perfsuite
                  COMMAND ${_ep_perfsuite_cmdline}
                  DEPENDS ${CMAKE_BINARY_DIR}/memcached/engine_testapp
                          ep
                          ep_perfsuite
                  VERBATIM)
ADD_TEST(NAME ep-engine_perfsuite
         COMMAND ${_ep_perfsuite_cmdline} -e "dbname=./value_eviction_perf")

# Micro perf tests: full eviction mode
ADD_CUSTOM_TARGET(test-full-eviction-perfsuite
                  COMMAND ${_ep_perfsuite_cmdline} -e "item_eviction_policy=full_eviction"
                  DEPENDS ${CMAKE_BINARY_DIR}/memcached/engine_testapp
                          ep
                          ep_perfsuite
                  VERBATIM)
ADD_TEST(NAME ep-engine_full_eviction_perfsuite
         COMMAND ${_ep_perfsuite_cmdline} -e "item_eviction_policy=full_eviction;dbname=./full_eviction_perf")
# ============================================================================ #

ENABLE_CODE_COVERAGE_REPORT()
