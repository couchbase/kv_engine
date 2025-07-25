include(CouchbaseGoogleTest)

add_compile_options_disable_optimization()

ADD_LIBRARY(mock_dcp EXCLUDE_FROM_ALL OBJECT mock/mock_dcp.cc)
SET_PROPERTY(TARGET mock_dcp PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(mock_dcp)
add_sanitizers(mock_dcp)
target_link_libraries(mock_dcp PUBLIC platform)

ADD_LIBRARY(ep_testsuite_objs EXCLUDE_FROM_ALL OBJECT
        ep_request_utils.cc
        ep_testsuite_common.cc
        ep_test_apis.cc)
SET_PROPERTY(TARGET ep_testsuite_objs PROPERTY POSITION_INDEPENDENT_CODE 1)
add_sanitizers(ep_testsuite_objs)
target_link_libraries(ep_testsuite_objs PUBLIC platform ep)

# Single executable containing all class-level unit tests involving
# EventuallyPersistentEngine driven by GoogleTest.
# (We end up compiling most of the src/ files of ep-engine for these unit tests,
# so simpler / quicker just to link them into a single executable).
cb_add_test_executable(ep-engine_ep_unit_tests
        ep_request_utils.cc
        module_tests/access_scanner_test.cc
        module_tests/array_histogram_test.cc
        module_tests/atomic_unordered_map_test.cc
        module_tests/backfill-manager_test.cc
        module_tests/backfill_test.cc
        module_tests/backfill_disk_test.cc
        module_tests/basic_ll_test.cc
        module_tests/bloomfilter_test.cc
        module_tests/bucket_logger_engine_test.cc
        module_tests/bucket_logger_test.cc
        module_tests/bucket_quota_change_test.cc
        module_tests/checkpoint_durability_test.cc
        module_tests/checkpoint_expel.cc
        module_tests/checkpoint_iterator_test.cc
        module_tests/checkpoint_remover_test.h
        module_tests/checkpoint_remover_test.cc
        module_tests/checkpoint_test.h
        module_tests/checkpoint_test.cc
        module_tests/checkpoint_utils.h
        module_tests/pdm_utils.h
        module_tests/collections/collections_dcp_test.cc
        module_tests/collections/collections_dcp_producers.cc
        module_tests/collections/collections_kvstore_test.cc
        module_tests/collections/collections_oso_dcp_test.cc
        module_tests/collections/collections_seqno_advance_test.cc
        module_tests/collections/evp_store_collections_dcp_test.cc
        module_tests/collections/evp_store_collections_legacy_dcp_tests.cc
        module_tests/collections/evp_store_durability_collections_dcp_test.h
        module_tests/collections/evp_store_durability_collections_dcp_test.cc
        module_tests/collections/evp_store_collections_dcp_stream_test.cc
        module_tests/collections/evp_store_collections_eraser_test.cc
        module_tests/collections/evp_store_collections_test.cc
        module_tests/collections/filter_test.cc
        module_tests/collections/manifest_test.cc
        module_tests/collections/stat_checker.cc
        module_tests/collections/vbucket_manifest_test.cc
        module_tests/collections/vbucket_manifest_entry_test.cc
        module_tests/configuration_test.cc
        module_tests/config.cc
        module_tests/config_test.cc
        module_tests/conflict_resolution_test.cc
        module_tests/conn_store_test.cc
        module_tests/couch-kvstore_test.cc
        module_tests/couch_vbucket_encryption_keys_manager_test.cc
        module_tests/couchstore_bucket_tests.cc
        module_tests/defragmenter_test.cc
        module_tests/dcp_cache_transfer_test.cc
        module_tests/dcp_durability_stream_test.cc
        module_tests/dcp_hlc_invalid_strategy_test.cc
        module_tests/dcp_producer_config.cc
        module_tests/dcp_reflection_test.cc
        module_tests/dcp_response_test.cc
        module_tests/dcp_single_threaded_test.cc
        module_tests/dcp_stream_request_config.cc
        module_tests/dcp_stream_test.cc
        module_tests/dcp_stream_ephemeral_test.cc
        module_tests/dcp_stream_sync_repl_test.cc
        module_tests/dcp_test.cc
        module_tests/dcp_utils.cc
        module_tests/diskdockey_test.cc
        module_tests/doc_pre_expiry_test.cc
        module_tests/durability_monitor_test.cc
        module_tests/ep_bucket_tests.cc
        module_tests/ep_engine_group_test.cc
        module_tests/ep_types_log_test.cc
        module_tests/ep_unit_tests_main.cc
        module_tests/ephemeral_bucket_test.cc
        module_tests/ephemeral_vb_test.cc
        module_tests/evp_engine_test.cc
        module_tests/evp_store_durability_test.cc
        module_tests/evp_store_rollback_test.cc
        module_tests/evp_store_test.cc
        module_tests/evp_store_pause_resume_test.cc
        module_tests/evp_store_single_threaded_test.cc
        module_tests/evp_store_warmup_test.cc
        module_tests/evp_store_with_meta.cc
        module_tests/evp_vbucket_test.cc
        module_tests/executorpool_test.cc
        module_tests/failover_table_test.cc
        module_tests/file_cache_test.cc
        module_tests/file_ops_tracker_test.cc
        module_tests/flusher_test.cc
        module_tests/futurequeue_test.cc
        module_tests/hash_table_eviction_test.cc
        module_tests/hash_table_perspective_test.cc
        module_tests/hash_table_test.cc
        module_tests/hlc_test.cc
        module_tests/initial_mfu_test.cc
        module_tests/item_compressor_test.cc
        module_tests/item_eviction_test.cc
        module_tests/item_pager_test.cc
        module_tests/item_test.cc
        module_tests/kvstore_fuzz_test.cc
        module_tests/kvstore_test.cc
        module_tests/kvstore_error_injection_test.cc
        module_tests/kv_bucket_test.cc
        module_tests/nexus_kvstore_test.cc
        module_tests/mutation_log_test.cc
        module_tests/objectregistry_test.cc
        module_tests/mutex_test.cc
        module_tests/packed_ptr_test.cc
        module_tests/probabilistic_counter_test.cc
        module_tests/range_scan_test.cc
        module_tests/secondary_warmup_test.cc
        module_tests/snapshots/cache_test.cc
        module_tests/snapshots/snapshot_engine_test.cc
        module_tests/snapshots/snapshots_test.cc
        module_tests/stats_test.cc
        module_tests/storeddockey_test.cc
        module_tests/stored_value_test.cc
        module_tests/stream_container_test.cc
        module_tests/systemevent_test.cc
        module_tests/tagged_ptr_test.cc
        module_tests/task_concurrency_test.cc
        module_tests/task_signal_test.cc
        module_tests/test_helpers.cc
        module_tests/utilities/auto_refreshed_value_test.cc
        module_tests/utilities/weak_ptr_bag_test.cc
        module_tests/vb_adapters_test.cc
        module_tests/vb_filter_test.cc
        module_tests/vbucket_test.cc
        module_tests/vbucket_durability_test.cc
        module_tests/vb_ready_queue_test.cc
        module_tests/work_sharding_test.cc
        module_tests/warmup_test.cc
        module_tests/item_compression_task_test.cc
        $<TARGET_OBJECTS:couchstore_test_fileops>
        ${Memcached_SOURCE_DIR}/logger/logger_test_fixture.cc)
kv_enable_pch(ep-engine_ep_unit_tests)
IF(EP_USE_MAGMA)
    target_compile_definitions(ep-engine_ep_unit_tests PUBLIC HAVE_MAGMA_SUPPORT=1)
    target_sources(ep-engine_ep_unit_tests PUBLIC
            module_tests/continuous_backup_test.cc
            module_tests/history_dcp_test.cc
            module_tests/history_scan_test.cc
            module_tests/magma_bucket_tests.cc
            module_tests/magma-kvstore_test.cc)
ENDIF()
cb_enable_unity_build(ep-engine_ep_unit_tests)
set_target_properties(ep-engine_ep_unit_tests PROPERTIES UNITY_BUILD_BATCH_SIZE 4)

TARGET_INCLUDE_DIRECTORIES(ep-engine_ep_unit_tests
        PUBLIC
        ${Couchstore_SOURCE_DIR}
        ${EventuallyPersistentEngine_BINARY_DIR}/tests/include)

target_link_libraries(ep-engine_ep_unit_tests PRIVATE
        ep
        mock_dcp
        ep_mocks
        GTest::gtest
        GTest::gmock
        mcd_test_util
        mock_server
        platform_cb_malloc_arena
        phosphor
        ${EP_STORAGE_LIBS})
if (NUMA_FOUND)
    target_link_libraries(ep-engine_ep_unit_tests PRIVATE ${NUMA_LIBRARIES})
endif()

# Fuzz tests are linked into the ep-engine_ep_unit_tests executable.
if(HAVE_FUZZTEST)
    cb_link_fuzztest(ep-engine_ep_unit_tests)
    add_subdirectory(fuzz_tests)
endif()

if(FUZZTEST_FUZZING_MODE)
    # Add all fuzz tests under the fuzztest.ep-engine_ep_unit_tests prefix.
    # To run the fuzz tests, the FLAGS_fuzz_for env variable must be set.
    gtest_discover_tests(ep-engine_ep_unit_tests
            EXTRA_ARGS --fromenv=fuzz_for
            WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
            TEST_PREFIX fuzztest.ep-engine_ep_unit_tests.
            TEST_FILTER *FuzzTest*
            DISCOVERY_TIMEOUT 60)
endif()

add_sanitizers(ep-engine_ep_unit_tests)

cb_add_test_executable(ep-engine_couch-fs-stats_test
        module_tests/couch-fs-stats_test.cc
        ${Couchstore_SOURCE_DIR}/src/crc32.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/configuration.h
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/ep_time.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/file_ops_tracker.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/kvstore/couch-kvstore/couch-fs-stats.cc
        ${Memcached_SOURCE_DIR}/executor/globaltask.cc
        ${Memcached_SOURCE_DIR}/executor/tracer.cc
        $<TARGET_OBJECTS:couchstore_wrapped_fileops_test_framework>)
kv_enable_pch(ep-engine_couch-fs-stats_test)
TARGET_INCLUDE_DIRECTORIES(ep-engine_couch-fs-stats_test
        PRIVATE
        ${Couchstore_SOURCE_DIR}
        ${Couchstore_SOURCE_DIR}/src)
target_link_libraries(ep-engine_couch-fs-stats_test PRIVATE
        GTest::gtest
        GTest::gtest_main
        GTest::gmock
        mcd_util
        mock_server
        platform
        hdrhistogram
        phosphor)
add_sanitizers(ep-engine_couch-fs-stats_test)

cb_add_test_executable(ep-engine_string_utils_test
        module_tests/string_utils_test.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/string_utils.cc)
kv_enable_pch(ep-engine_string_utils_test)
target_link_libraries(ep-engine_string_utils_test PRIVATE GTest::gtest GTest::gtest_main platform)

set(slow_tests "*RangeScanCreateAndContinueTest*")
set(slow_test_timeout_seconds 1200)
set(default_timeout_seconds 300)

ADD_TEST(NAME ep-engine_couch-fs-stats_test COMMAND ep-engine_couch-fs-stats_test)
gtest_discover_tests(ep-engine_ep_unit_tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        TEST_PREFIX ep-engine_ep_unit_tests.
        TEST_FILTER "*:-${slow_tests}*"
        ONE_CTEST_PER_SUITE
        DISCOVERY_TIMEOUT 60
        PROPERTIES TIMEOUT ${default_timeout_seconds}
        TIMEOUT_SIGNAL_NAME SIGQUIT
                   ENVIRONMENT ${GTEST_ALL_SANITIZERS_ENV})
gtest_discover_tests(ep-engine_ep_unit_tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        TEST_PREFIX ep-engine_ep_unit_tests.
        TEST_FILTER "${slow_tests}"
        ONE_CTEST_PER_SUITE
        DISCOVERY_TIMEOUT 60
        PROPERTIES TIMEOUT ${slow_test_timeout_seconds}
        TIMEOUT_SIGNAL_NAME SIGQUIT
                   ENVIRONMENT ${GTEST_ALL_SANITIZERS_ENV})
IF (COUCHBASE_MAGMA_COMMIT_VALIDATION)
    # Create a set of tests for magma which are filtered such that only the
    # magma variants should run
    gtest_discover_tests(ep-engine_ep_unit_tests
            WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
            TEST_PREFIX ep-engine_ep_unit_tests_magma.
            ONE_CTEST_PER_SUITE
            # Magma variants have either Magma or magma in the names so it's
            # easier to just supply "agma" to the filter to capture both.
            POST_SUITE_FILTER *agma
            DISCOVERY_TIMEOUT 60
            PROPERTIES TIMEOUT 300
                       ENVIRONMENT ${GTEST_ALL_SANITIZERS_ENV})
ENDIF()

add_ep_testsuite(ep_testsuite ep_testsuite.cc)
add_ep_testsuite(ep_testsuite_basic ep_testsuite_basic.cc)
add_ep_testsuite(ep_testsuite_dcp ep_testsuite_dcp.cc)
add_ep_testsuite(ep_testsuite_checkpoint ep_testsuite_checkpoint.cc)
add_ep_testsuite(ep_testsuite_xdcr ep_testsuite_xdcr.cc)

# Defines a testsuite which runs in full and value eviction
# (persistent), ephemeral buckets (which has no eviction).
FUNCTION(ADD_TESTSUITE name timeout)
    SET(_cmdline $<TARGET_FILE:${name}> )

    # Sanitizers slow down test execution, in the region of 2x - 4x
    # slowdown. Given we don't want to trigger spurious false
    # positives in those configs, increase the specified timeout by 4x
    # if sanitizers are in use.
    IF (CB_SANITIZERS)
        MATH(EXPR timeout "${timeout}*4")
    ENDIF()

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

    foreach (eviction IN ITEMS value_eviction full_eviction)
        foreach(comp IN ITEMS comp_passive comp_active)
            set_tests_properties(${name}.${eviction}.${comp} PROPERTIES
                    TIMEOUT ${timeout}
                    ENVIRONMENT CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT=1)
        endforeach()
    endforeach ()

    ADD_TEST(NAME ${name}.ephemeral.comp_passive
            COMMAND ${_cmdline} -v -e "compression_mode=passive$<SEMICOLON>bucket_type=ephemeral\
$<SEMICOLON>dbname=./${name}.ephemeral.comp_passive.db")
    ADD_TEST(NAME ${name}.ephemeral.comp_active
            COMMAND ${_cmdline} -v -e "compression_mode=active$<SEMICOLON>bucket_type=ephemeral\
$<SEMICOLON>dbname=./${name}.ephemeral.comp_active.db")
    foreach(comp IN ITEMS comp_passive comp_active)
        set_tests_properties(${name}.ephemeral.${comp} PROPERTIES
                TIMEOUT ${timeout}
                ENVIRONMENT CB_ARENA_MALLOC_VERIFY_DEALLOC_CLIENT=1)
    endforeach()

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
IF(WIN32)
    # Increased timeout on Windows as suite is slower on CV there (IO subsystem
    # on CV machines slower?)
    ADD_TESTSUITE(ep_testsuite 600 SKIP_MAGMA)
ELSE()
    ADD_TESTSUITE(ep_testsuite 300)
ENDIF()

ADD_TESTSUITE(ep_testsuite_basic 240)

ADD_TESTSUITE(ep_testsuite_dcp 240)

ADD_TESTSUITE(ep_testsuite_checkpoint 180)

ADD_TESTSUITE(ep_testsuite_xdcr 120)

# ================================ PERF_TESTS ================================ #
SET(_ep_perfsuite_cmdline $<TARGET_FILE:ep_perfsuite> -v)

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
