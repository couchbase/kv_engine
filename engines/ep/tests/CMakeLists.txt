include(CouchbaseGoogleTest)

add_compile_options_disable_optimization()

ADD_LIBRARY(mock_dcp OBJECT mock/mock_dcp.cc)
SET_PROPERTY(TARGET mock_dcp PROPERTY POSITION_INDEPENDENT_CODE 1)
add_sanitizers(mock_dcp)

ADD_LIBRARY(ep_testsuite_objs OBJECT
        ep_request_utils.cc
        ep_testsuite_common.cc
        ep_test_apis.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/ext_meta_parser.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/storeddockey.cc)
SET_PROPERTY(TARGET ep_testsuite_objs PROPERTY POSITION_INDEPENDENT_CODE 1)
add_sanitizers(ep_testsuite_objs)

# Single executable containing all class-level unit tests involving
# EventuallyPersistentEngine driven by GoogleTest.
# (We end up compiling most of the src/ files of ep-engine for these unit tests,
# so simpler / quicker just to link them into a single executable).
ADD_EXECUTABLE(ep-engine_ep_unit_tests
        ep_request_utils.cc
        module_tests/atomic_unordered_map_test.cc
        module_tests/basic_ll_test.cc
        module_tests/bloomfilter_test.cc
        module_tests/bucket_logger_engine_test.cc
        module_tests/bucket_logger_test.cc
        module_tests/checkpoint_durability_test.cc
        module_tests/checkpoint_iterator_test.cc
        module_tests/checkpoint_remover_test.h
        module_tests/checkpoint_remover_test.cc
        module_tests/checkpoint_test.h
        module_tests/checkpoint_test.cc
        module_tests/checkpoint_utils.h
        module_tests/collections/collections_dcp_test.cc
        module_tests/collections/collections_kvstore_test.cc
        module_tests/collections/collections_oso_dcp_test.cc
        module_tests/collections/evp_store_collections_dcp_test.cc
        module_tests/collections/evp_store_collections_dcp_stream_test.cc
        module_tests/collections/evp_store_collections_eraser_test.cc
        module_tests/collections/evp_store_collections_test.cc
        module_tests/collections/filter_test.cc
        module_tests/collections/manifest_test.cc
        module_tests/collections/test_manifest.cc
        module_tests/collections/vbucket_manifest_test.cc
        module_tests/collections/vbucket_manifest_entry_test.cc
        module_tests/configuration_test.cc
        module_tests/conn_store_test.cc
        module_tests/defragmenter_test.cc
        module_tests/dcp_durability_stream_test.cc
        module_tests/dcp_reflection_test.cc
        module_tests/dcp_stream_test.cc
        module_tests/dcp_stream_sync_repl_test.cc
        module_tests/dcp_test.cc
        module_tests/dcp_utils.cc
        module_tests/diskdockey_test.cc
        module_tests/durability_monitor_test.cc
        module_tests/ep_unit_tests_main.cc
        module_tests/ephemeral_bucket_test.cc
        module_tests/ephemeral_vb_test.cc
        module_tests/evp_engine_test.cc
        module_tests/evp_store_durability_test.cc
        module_tests/evp_store_rollback_test.cc
        module_tests/evp_store_test.cc
        module_tests/evp_store_single_threaded_test.cc
        module_tests/evp_store_warmup_test.cc
        module_tests/evp_store_with_meta.cc
        module_tests/evp_vbucket_test.cc
        module_tests/executorpool_test.cc
        module_tests/failover_table_test.cc
        module_tests/flusher_test.cc
        module_tests/futurequeue_test.cc
        module_tests/hash_table_eviction_test.cc
        module_tests/hash_table_perspective_test.cc
        module_tests/hash_table_test.cc
        module_tests/hdrhistogram_test.cc
        module_tests/item_compressor_test.cc
        module_tests/item_eviction_test.cc
        module_tests/item_pager_test.cc
        module_tests/item_test.cc
        module_tests/kvstore_test.cc
        module_tests/kv_bucket_test.cc
        module_tests/memory_tracking_allocator_test.cc
        module_tests/monotonic_test.cc
        module_tests/mutation_log_test.cc
        module_tests/objectregistry_test.cc
        module_tests/mutex_test.cc
        module_tests/probabilistic_counter_test.cc
        module_tests/stats_test.cc
        module_tests/storeddockey_test.cc
        module_tests/stored_value_test.cc
        module_tests/stream_container_test.cc
        module_tests/systemevent_test.cc
        module_tests/tagged_ptr_test.cc
        module_tests/test_helpers.cc
        module_tests/vbucket_test.cc
        module_tests/vbucket_durability_test.cc
        module_tests/warmup_test.cc
        $<TARGET_OBJECTS:mock_dcp>
        $<TARGET_OBJECTS:ep_objs>
        $<TARGET_OBJECTS:ep_mocks>
        $<TARGET_OBJECTS:couchstore_test_fileops>
        $<TARGET_OBJECTS:couchfile_upgrade_objects>
        ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
        ${Memcached_SOURCE_DIR}/logger/logger_test_fixture.cc)

TARGET_INCLUDE_DIRECTORIES(ep-engine_ep_unit_tests
        PUBLIC
        ${Couchstore_SOURCE_DIR}
        ${EventuallyPersistentEngine_BINARY_DIR}/tests/include)

TARGET_LINK_LIBRARIES(ep-engine_ep_unit_tests
        cbcompress
        dirutils
        engine_utilities
        ep-engine_collections
        gtest
        gmock
        hdr_histogram_static
        JSON_checker
        memcached_logger
        mcbp
        mcd_util
        mcd_tracing
        mock_server
        platform
        platform_cb_malloc_arena
        phosphor
        xattr
        ${EP_STORAGE_LIBS}
        ${MALLOC_LIBRARIES}
        ${LIBEVENT_LIBRARIES}
        ${FOLLY_LIBRARIES})

if (NUMA_FOUND)
    target_link_libraries(ep-engine_ep_unit_tests ${NUMA_LIBRARIES})
endif()

add_sanitizers(ep-engine_ep_unit_tests)

ADD_EXECUTABLE(ep-engine_atomic_ptr_test
        module_tests/atomic_ptr_test.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/atomic.cc)
TARGET_LINK_LIBRARIES(ep-engine_atomic_ptr_test platform platform_cb_malloc_arena)

ADD_EXECUTABLE(ep-engine_couch-fs-stats_test
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/couch-kvstore/couch-fs-stats.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/configuration.h
        module_tests/couch-fs-stats_test.cc
        ${Couchstore_SOURCE_DIR}/src/crc32.cc
        $<TARGET_OBJECTS:couchstore_wrapped_fileops_test_framework>)
TARGET_INCLUDE_DIRECTORIES(ep-engine_couch-fs-stats_test
        PRIVATE
        ${Couchstore_SOURCE_DIR}
        ${Couchstore_SOURCE_DIR}/src)
TARGET_LINK_LIBRARIES(ep-engine_couch-fs-stats_test
        gtest
        gtest_main
        gmock
        mcd_util
        platform
        phosphor)
add_sanitizers(ep-engine_couch-fs-stats_test)

ADD_EXECUTABLE(ep-engine_misc_test module_tests/misc_test.cc)
TARGET_LINK_LIBRARIES(ep-engine_misc_test mcbp platform)

ADD_EXECUTABLE(ep-engine_string_utils_test
        module_tests/string_utils_test.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/string_utils.cc)
TARGET_LINK_LIBRARIES(ep-engine_string_utils_test gtest gtest_main platform)

ADD_TEST(NAME ep-engine_atomic_ptr_test COMMAND ep-engine_atomic_ptr_test)
ADD_TEST(NAME ep-engine_couch-fs-stats_test COMMAND ep-engine_couch-fs-stats_test)
gtest_discover_tests(ep-engine_ep_unit_tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        TEST_PREFIX ep-engine_ep_unit_tests.
        ONE_CTEST_PER_SUITE
        DISCOVERY_TIMEOUT 60
        PROPERTIES TIMEOUT 300)

ADD_TEST(NAME ep-engine_misc_test COMMAND ep-engine_misc_test)

ADD_EXECUTABLE(ep_testsuite
               ep_testsuite.cc
               $<TARGET_OBJECTS:ep_testsuite_objs>
               $<TARGET_OBJECTS:mock_dcp>
               $<TARGET_OBJECTS:engine_testapp>)
TARGET_LINK_LIBRARIES(ep_testsuite
                      engine_testapp_dependencies
                      engine_utilities
                      mcbp
                      mcd_util
                      ${EP_STORAGE_LIBS}
                      cbcompress
                      dirutils
                      JSON_checker
                      platform
                      xattr
                      ${LIBEVENT_LIBRARIES})
add_sanitizers(ep_testsuite)

ADD_EXECUTABLE(ep_testsuite_basic
               ep_testsuite_basic.cc
               $<TARGET_OBJECTS:ep_testsuite_objs>
               $<TARGET_OBJECTS:mock_dcp>
               $<TARGET_OBJECTS:engine_testapp>)
TARGET_LINK_LIBRARIES(ep_testsuite_basic
                      engine_testapp_dependencies
                      cbcompress
                      dirutils
                      engine_utilities
                      JSON_checker
                      mcbp
                      mcd_util
                      platform
                      ${LIBEVENT_LIBRARIES}
                      ${SNAPPY_LIBRARIES})
add_sanitizers(ep_testsuite_basic)

ADD_EXECUTABLE(ep_testsuite_dcp
               ep_testsuite_dcp.cc
               $<TARGET_OBJECTS:ep_testsuite_objs>
               $<TARGET_OBJECTS:mock_dcp>
               $<TARGET_OBJECTS:engine_testapp>)
TARGET_LINK_LIBRARIES(ep_testsuite_dcp
                      engine_testapp_dependencies
                      cbcompress
                      dirutils
                      engine_utilities
                      JSON_checker
                      mcbp
                      mcd_util
                      platform
                      ${LIBEVENT_LIBRARIES}
                      ${SNAPPY_LIBRARIES})
add_sanitizers(ep_testsuite_dcp)

ADD_EXECUTABLE(ep_testsuite_checkpoint
               ep_testsuite_checkpoint.cc
               $<TARGET_OBJECTS:ep_testsuite_objs>
               $<TARGET_OBJECTS:engine_testapp>)
TARGET_LINK_LIBRARIES(ep_testsuite_checkpoint
                      engine_testapp_dependencies
                      cbcompress
                      dirutils
                      engine_utilities
                      JSON_checker
                      mcbp
                      mcd_util
                      platform
                      ${LIBEVENT_LIBRARIES}
                      ${SNAPPY_LIBRARIES})
add_sanitizers(ep_testsuite_checkpoint)

ADD_EXECUTABLE(ep_testsuite_xdcr
               ep_testsuite_xdcr.cc
               $<TARGET_OBJECTS:ep_testsuite_objs>
               $<TARGET_OBJECTS:engine_testapp>)
TARGET_LINK_LIBRARIES(ep_testsuite_xdcr
                      engine_testapp_dependencies mcd_util engine_utilities
                      mcbp JSON_checker dirutils platform xattr
                      ${LIBEVENT_LIBRARIES} ${SNAPPY_LIBRARIES})
add_sanitizers(ep_testsuite_xdcr)

# MB-35234: Currently broken due to data race during warmup when enabling
# durability.
IF(FALSE)
    IF (EP_USE_ROCKSDB)
        ADD_TEST(NAME ep-engine-persistence-unit-tests.rocksdb
                COMMAND memcached_testapp -E ep -c "backend=rocksdb" --gtest_filter=*PersistToTest*)
        SET_TESTS_PROPERTIES(ep-engine-persistence-unit-tests.rocksdb PROPERTIES TIMEOUT 300)
    ENDIF (EP_USE_ROCKSDB)
ENDIF()
# Defines a testsuite which runs in full and value eviction
# (persistent), ephemeral buckets (which has no eviction).
FUNCTION(ADD_TESTSUITE name timeout)
    SET(_cmdline $<TARGET_FILE:${name}> -E ep)

    # MB-34445: To attempt to identify the cause of the intermittent timeouts in
    # the ephemeral testsuites, use the 'timeout' program to signal ep_testsuite
    # with SIGABRT when we timeout; to allow a core dump to be generated of
    # where the process is stuck. Use the 'timeout' program if applicable. If
    # not, we just pass the timeout value into the CTest timeout feature.
    find_program (TIMEOUT_EXE timeout)
    IF (TIMEOUT_EXE AND NOT WIN32)
        SET(_cmdline ${TIMEOUT_EXE} --preserve-status --signal=ABRT
                     ${timeout}s ${_cmdline})
        # Ideally, we would just not set the TIMEOUT property if we can use
        # the 'timeout' program, however, it defaults to the value 1500 if we
        # don't set it which may be smaller than the timeout that we pass into
        # this function. So, just double the TIMEOUT value that we set so that
        # it won't interfere with the 'timeout' programs actual timeout.
        MATH(EXPR timeout "${timeout}*2")
    ENDIF()

    ADD_TEST(NAME ${name}.value_eviction.comp_passive
            COMMAND ${_cmdline} -v -e "compression_mode=passive$<SEMICOLON>dbname=./${name}.value_eviction.comp_passive.db")
    ADD_TEST(NAME ${name}.value_eviction.comp_active
            COMMAND ${_cmdline} -v -e "compression_mode=active$<SEMICOLON>dbname=./${name}.value_eviction.comp_active.db")
    ADD_TEST(NAME ${name}.full_eviction.comp_passive
            COMMAND ${_cmdline} -v -e "compression_mode=passive$<SEMICOLON>item_eviction_policy=full_eviction\
$<SEMICOLON>dbname=./${name}.full_eviction.comp_passive.db")
    ADD_TEST(NAME ${name}.full_eviction.comp_active
            COMMAND ${_cmdline} -v -e "compression_mode=active$<SEMICOLON>item_eviction_policy=full_eviction\
$<SEMICOLON>dbname=./${name}.full_eviction.comp_active.db")
    SET_TESTS_PROPERTIES(${name}.value_eviction.comp_passive PROPERTIES TIMEOUT ${timeout})
    SET_TESTS_PROPERTIES(${name}.value_eviction.comp_active PROPERTIES TIMEOUT ${timeout})
    SET_TESTS_PROPERTIES(${name}.full_eviction.comp_passive PROPERTIES TIMEOUT ${timeout})
    SET_TESTS_PROPERTIES(${name}.full_eviction.comp_active PROPERTIES TIMEOUT ${timeout})

    ADD_TEST(NAME ${name}.ephemeral.comp_passive
            COMMAND ${_cmdline} -v -e "compression_mode=passive$<SEMICOLON>bucket_type=ephemeral\
$<SEMICOLON>dbname=./${name}.ephemeral.comp_passive.db")
    ADD_TEST(NAME ${name}.ephemeral.comp_active
            COMMAND ${_cmdline} -v -e "compression_mode=active$<SEMICOLON>bucket_type=ephemeral\
$<SEMICOLON>dbname=./${name}.ephemeral.comp_active.db")
    SET_TESTS_PROPERTIES(${name}.ephemeral.comp_passive PROPERTIES TIMEOUT ${timeout})
    SET_TESTS_PROPERTIES(${name}.ephemeral.comp_active PROPERTIES TIMEOUT ${timeout})

    IF (EP_USE_ROCKSDB)
        ADD_TEST(NAME ${name}.value_eviction.rocksdb
                COMMAND ${_cmdline} -v -e "dbname=./${name}.value_eviction.rocksdb$<SEMICOLON>backend=rocksdb")
        ADD_TEST(NAME ${name}.full_eviction.rocksdb
                COMMAND ${_cmdline} -v -e "item_eviction_policy=full_eviction$<SEMICOLON>dbname=./${name}.full_eviction.rocksdb$<SEMICOLON>backend=rocksdb")
        SET_TESTS_PROPERTIES(${name}.value_eviction.rocksdb PROPERTIES TIMEOUT ${timeout})
        SET_TESTS_PROPERTIES(${name}.full_eviction.rocksdb PROPERTIES TIMEOUT ${timeout})
    ENDIF (EP_USE_ROCKSDB)

    IF (EP_USE_MAGMA)
        ADD_TEST(NAME ${name}.value_eviction.magma
                COMMAND ${_cmdline} -v -e "dbname=./${name}.value_eviction.magma$<SEMICOLON>backend=magma")
        ADD_TEST(NAME ${name}.full_eviction.magma
                COMMAND ${_cmdline} -v -e "item_eviction_policy=full_eviction$<SEMICOLON>dbname=./${name}.full_eviction.magma$<SEMICOLON>backend=magma")
        SET_TESTS_PROPERTIES(${name}.value_eviction.magma PROPERTIES TIMEOUT ${timeout})
        SET_TESTS_PROPERTIES(${name}.full_eviction.magma PROPERTIES TIMEOUT ${timeout})
    ENDIF ()
ENDFUNCTION()

# MB-36252: Skipping ep_testsuite under Magma as intermittently times out on
# Windows
# MB-36322: Skipping ep_testsuite under TSan + Magma as
# intermittently timing out.
IF(WIN32 OR CB_THREADSANITIZER)
    # Increased timeout on Windows as suite is slower on CV there (IO subsystem
    # on CV machines slower?)
    ADD_TESTSUITE(ep_testsuite 600 SKIP_MAGMA)
ELSE()
    ADD_TESTSUITE(ep_testsuite 300)
ENDIF()

ADD_TESTSUITE(ep_testsuite_basic 600)

ADD_TESTSUITE(ep_testsuite_dcp 1200)

ADD_TESTSUITE(ep_testsuite_checkpoint 180)

ADD_TESTSUITE(ep_testsuite_xdcr 120)

# ================================ PERF_TESTS ================================ #
SET(_ep_perfsuite_cmdline $<TARGET_FILE:ep_perfsuite> -E ep -v)

# Micro perf tests: value-only eviction mode. Note that it makes no sense to run
# these tests in full-eviction mode as we disable persistence - see MB-19504.
ADD_CUSTOM_TARGET(test-perfsuite
        COMMAND ${_ep_perfsuite_cmdline}
        DEPENDS engine_testapp
        ep
        ep_perfsuite
        VERBATIM)
ADD_TEST(NAME ep_perfsuite.value_eviction
        COMMAND ${_ep_perfsuite_cmdline} -e "dbname=./ep_perfsuite.value_eviction.db")

ADD_TEST(NAME ep_perfsuite.ephemeral
        COMMAND ${_ep_perfsuite_cmdline} -e "bucket_type=ephemeral$<SEMICOLON>dbname=./ep_perfsuite.ephemeral.db")

# ============================================================================ #

configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/module_tests/test_file_helper.h.in
        ${EventuallyPersistentEngine_BINARY_DIR}/tests/include/test_file_helper.h)
