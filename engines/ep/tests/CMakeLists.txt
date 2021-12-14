include(CouchbaseGoogleTest)

add_compile_options_disable_optimization()

ADD_LIBRARY(mock_dcp OBJECT mock/mock_dcp.cc)
SET_PROPERTY(TARGET mock_dcp PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(mock_dcp)
add_sanitizers(mock_dcp)
target_link_libraries(mock_dcp PUBLIC platform)

ADD_LIBRARY(ep_testsuite_objs OBJECT
        ep_request_utils.cc
        ep_testsuite_common.cc
        ep_test_apis.cc
        $<TARGET_OBJECTS:ep_objs>)
SET_PROPERTY(TARGET ep_testsuite_objs PROPERTY POSITION_INDEPENDENT_CODE 1)
add_sanitizers(ep_testsuite_objs)
target_link_libraries(ep_testsuite_objs PUBLIC platform)

# Single executable containing all class-level unit tests involving
# EventuallyPersistentEngine driven by GoogleTest.
# (We end up compiling most of the src/ files of ep-engine for these unit tests,
# so simpler / quicker just to link them into a single executable).
cb_add_test_executable(ep-engine_ep_unit_tests
        ep_request_utils.cc
        module_tests/atomic_unordered_map_test.cc
        module_tests/backfill-manager_test.cc
        module_tests/backfill_disk_test.cc
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
        module_tests/collections/collections_dcp_producers.cc
        module_tests/collections/collections_kvstore_test.cc
        module_tests/collections/collections_oso_dcp_test.cc
        module_tests/collections/collections_seqno_advance.cc
        module_tests/collections/evp_store_collections_dcp_test.cc
        module_tests/collections/evp_store_collections_legacy_dcp_tests.cc
        module_tests/collections/evp_store_durability_collections_dcp_test.h
        module_tests/collections/evp_store_durability_collections_dcp_test.cc
        module_tests/collections/evp_store_collections_dcp_stream_test.cc
        module_tests/collections/evp_store_collections_eraser_test.cc
        module_tests/collections/evp_store_collections_test.cc
        module_tests/collections/filter_test.cc
        module_tests/collections/manifest_test.cc
        module_tests/collections/manifest_update_test.cc
        module_tests/collections/scope_data_limit_tests.cc
        module_tests/collections/stat_checker.cc
        module_tests/collections/vbucket_manifest_test.cc
        module_tests/collections/vbucket_manifest_entry_test.cc
        module_tests/configuration_test.cc
        module_tests/conn_store_test.cc
        module_tests/couch-kvstore_test.cc
        module_tests/couchstore_bucket_tests.cc
        module_tests/defragmenter_test.cc
        module_tests/dcp_durability_stream_test.cc
        module_tests/dcp_reflection_test.cc
        module_tests/dcp_response_test.cc
        module_tests/dcp_single_threaded_test.cc
        module_tests/dcp_stream_test.cc
        module_tests/dcp_stream_ephemeral_test.cc
        module_tests/dcp_stream_sync_repl_test.cc
        module_tests/dcp_test.cc
        module_tests/dcp_utils.cc
        module_tests/diskdockey_test.cc
        module_tests/durability_monitor_test.cc
        module_tests/ep_bucket_tests.cc
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
        module_tests/file_cache_test.cc
        module_tests/flusher_test.cc
        module_tests/futurequeue_test.cc
        module_tests/hash_table_eviction_test.cc
        module_tests/hash_table_perspective_test.cc
        module_tests/hash_table_test.cc
        module_tests/hlc_test.cc
        module_tests/item_compressor_test.cc
        module_tests/item_eviction_test.cc
        module_tests/item_pager_test.cc
        module_tests/item_test.cc
        module_tests/kvstore_test.cc
        module_tests/kvstore_error_injection_test.cc
        module_tests/kv_bucket_test.cc
        module_tests/nexus_kvstore_test.cc
        module_tests/mutation_log_test.cc
        module_tests/objectregistry_test.cc
        module_tests/mutex_test.cc
        module_tests/pitr_test.cc
        module_tests/probabilistic_counter_test.cc
        module_tests/range_scan_test.cc
        module_tests/stats_test.cc
        module_tests/storeddockey_test.cc
        module_tests/stored_value_test.cc
        module_tests/stream_container_test.cc
        module_tests/systemevent_test.cc
        module_tests/tagged_ptr_test.cc
        module_tests/task_concurrency_test.cc
        module_tests/test_helpers.cc
        module_tests/vbucket_test.cc
        module_tests/vbucket_durability_test.cc
        module_tests/vb_ready_queue_test.cc
        module_tests/warmup_test.cc
        $<TARGET_OBJECTS:mock_dcp>
        $<TARGET_OBJECTS:ep_objs>
        $<TARGET_OBJECTS:ep_mocks>
        $<TARGET_OBJECTS:couchstore_test_fileops>
        $<TARGET_OBJECTS:couchfile_upgrade_objects>
        ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
        ${Memcached_SOURCE_DIR}/logger/logger_test_fixture.cc)
kv_enable_pch(ep-engine_ep_unit_tests)
IF(EP_USE_MAGMA)
    target_sources(ep-engine_ep_unit_tests PUBLIC
            module_tests/magma_bucket_tests.cc
            module_tests/magma-kvstore_test.cc)
ENDIF()

TARGET_INCLUDE_DIRECTORIES(ep-engine_ep_unit_tests
        PUBLIC
        ${Couchstore_SOURCE_DIR}
        ${EventuallyPersistentEngine_BINARY_DIR}/tests/include)

target_link_libraries(ep-engine_ep_unit_tests PRIVATE
        cbcompress
        engine_utilities
        ep-engine_collections
        mcd_executor
        GTest::gtest
        GTest::gmock
        hdrhistogram
        memcached_logger
        mcbp
        mcd_util
        mcd_test_util
        mcd_time
        mcd_tracing
        mock_server
        platform
        platform_cb_malloc_arena
        phosphor
        statistics
        xattr
        ${EP_STORAGE_LIBS}
        ${MALLOC_LIBRARIES}
        ${LIBEVENT_LIBRARIES})

if (NUMA_FOUND)
    target_link_libraries(ep-engine_ep_unit_tests PRIVATE ${NUMA_LIBRARIES})
endif()

add_sanitizers(ep-engine_ep_unit_tests)
kv_enable_pch(ep-engine_ep_unit_tests)

cb_add_test_executable(ep-engine_couch-fs-stats_test
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/kvstore/couch-kvstore/couch-fs-stats.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/configuration.h
        module_tests/couch-fs-stats_test.cc
        ${Couchstore_SOURCE_DIR}/src/crc32.cc
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
        platform
        hdrhistogram
        phosphor)
add_sanitizers(ep-engine_couch-fs-stats_test)

cb_add_test_executable(ep-engine_misc_test module_tests/misc_test.cc)
kv_enable_pch(ep-engine_misc_test)
target_link_libraries(ep-engine_misc_test PRIVATE mcbp platform)

cb_add_test_executable(ep-engine_string_utils_test
        module_tests/string_utils_test.cc
        ${EventuallyPersistentEngine_SOURCE_DIR}/src/string_utils.cc)
kv_enable_pch(ep-engine_string_utils_test)
target_link_libraries(ep-engine_string_utils_test PRIVATE GTest::gtest GTest::gtest_main platform)

ADD_TEST(NAME ep-engine_couch-fs-stats_test COMMAND ep-engine_couch-fs-stats_test)
gtest_discover_tests(ep-engine_ep_unit_tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        TEST_PREFIX ep-engine_ep_unit_tests.
        ONE_CTEST_PER_SUITE
        DISCOVERY_TIMEOUT 60
        PROPERTIES TIMEOUT 300
        ENVIRONMENT ${THREAD_SANITIZER_TEST_ENV})
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
            ENVIRONMENT ${THREAD_SANITIZER_TEST_ENV})
ENDIF()

ADD_TEST(NAME ep-engine_misc_test COMMAND ep-engine_misc_test)

add_ep_testsuite(ep_testsuite ep_testsuite.cc)
add_ep_testsuite(ep_testsuite_basic ep_testsuite_basic.cc)
add_ep_testsuite(ep_testsuite_dcp ep_testsuite_dcp.cc)
add_ep_testsuite(ep_testsuite_checkpoint ep_testsuite_checkpoint.cc)
add_ep_testsuite(ep_testsuite_xdcr ep_testsuite_xdcr.cc)

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
