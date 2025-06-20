PROJECT(EventuallyPersistentEngine LANGUAGES CXX)

INCLUDE(CMakeDependentOption)
INCLUDE(CTest)

CMAKE_DEPENDENT_OPTION(EP_USE_MAGMA "Enable support for Magma" ON
    "MAGMA_INCLUDE_DIR" OFF)

INCLUDE_DIRECTORIES(BEFORE SYSTEM
                           ${Couchstore_SOURCE_DIR}/include
                           ${Couchstore_BINARY_DIR}/include)

INCLUDE_DIRECTORIES(BEFORE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_BINARY_DIR}/src
        # flatbuffers should be in the previous section
        # but using SYSTEM caused clang-analyze to report
        # a memory leak in one of the flatbuffers headers
        ${FLATBUFFERS_INCLUDE_DIR})

SET(EP_STORAGE_LIBS couchstore_static ep-engine_storage_common)

IF (EP_USE_MAGMA)
    INCLUDE_DIRECTORIES(AFTER ${MAGMA_INCLUDE_DIR})
    LIST(APPEND EP_STORAGE_LIBS magma)
    SET(MAGMA_KVSTORE_SOURCE
            src/kvstore/magma-kvstore/fusion_uploader.cc
            src/kvstore/magma-kvstore/magma-kvstore.cc
            src/kvstore/magma-kvstore/magma-kvstore_config.cc
            src/kvstore/magma-kvstore/magma-kvstore_fs.cc
            src/kvstore/magma-kvstore/magma-memory-tracking-proxy.cc)
    ADD_DEFINITIONS(-DEP_USE_MAGMA=1)
    # AppleClang 15 fails with C++23 as part of unity build for the memory
    # tracking proxy. Just compile it on its own until we can upgrade
    # the compiler
    set_source_files_properties(
            src/kvstore/magma-kvstore/magma-memory-tracking-proxy.cc
            PROPERTIES
            SKIP_UNITY_BUILD_INCLUSION ON)

    MESSAGE(STATUS "ep-engine: Building magma-kvstore")

    add_library(ep-engine_magma_common STATIC
            src/kvstore/magma-kvstore/kv_magma_common/magma-kvstore_metadata.cc
            src/kvstore/magma-kvstore/kv_magma_common/magma-kvstore_magma_db_stats.cc)
    target_link_libraries(ep-engine_magma_common PRIVATE platform)
    target_link_libraries(ep-engine_magma_common PUBLIC magma)
    target_include_directories(ep-engine_magma_common PUBLIC
            src/kvstore/magma-kvstore/kv_magma_common)
    set_property(TARGET ep-engine_magma_common PROPERTY POSITION_INDEPENDENT_CODE 1)
    kv_enable_pch(ep-engine_magma_common)
    list(APPEND EP_STORAGE_LIBS ep-engine_magma_common)
ENDIF (EP_USE_MAGMA)

# For debugging without compiler optimizations uncomment line below..
#SET (CMAKE_BUILD_TYPE DEBUG)

SET(EP_ENGINE_VERSION "${MEMCACHED_VERSION}")

# Build Python tools
ADD_SUBDIRECTORY(management)

add_executable(gencode tools/gencode.cc)
target_link_libraries(gencode PRIVATE nlohmann_json::nlohmann_json platform)
add_executable(genconfig tools/genconfig.cc)
target_link_libraries(genconfig PRIVATE fmt::fmt nlohmann_json::nlohmann_json platform)
add_sanitizers(genconfig)

add_executable(pid_runner tools/pid_runner.cc)
target_link_libraries(pid_runner PRIVATE ep)

add_subdirectory(tools/humpty_dumpty)

cb_add_test_executable(kvstore_gen tools/kvstore_gen.cc
        ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_cookie.cc
        ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc)
kv_enable_pch(kvstore_gen)
target_link_libraries(kvstore_gen PRIVATE ep)

ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
                          ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h
                  COMMAND
                     ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/src
                  COMMAND
                     gencode -j ${CMAKE_CURRENT_SOURCE_DIR}/docs/stats.json -h ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h -c ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c -f get_stats_info
                  DEPENDS
                        docs/stats.json
                        gencode
                  COMMENT "Generating code for stats info")

ADD_CUSTOM_COMMAND(OUTPUT
                     src/generated_configuration.cc.temp
                     src/generated_configuration.h.temp
                     src/generated_enums.cc.temp
                     src/generated_enums.h.temp
                  COMMAND
                     ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/src
                  COMMAND
                     genconfig ${CMAKE_CURRENT_SOURCE_DIR}/configuration.json src/generated_configuration.h.temp src/generated_configuration.cc.temp src/generated_enums.h.temp src/generated_enums.cc.temp
                  DEPENDS
                        configuration.json
                        genconfig
                  COMMENT "Generating code for configuration class")

ADD_CUSTOM_COMMAND(OUTPUT
                     src/generated_configuration.cc
                     src/generated_configuration.h
                     src/generated_enums.cc
                     src/generated_enums.h
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_configuration.cc.temp src/generated_configuration.cc
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_configuration.h.temp src/generated_configuration.h
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_enums.cc.temp src/generated_enums.cc
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_enums.h.temp src/generated_enums.h
                  DEPENDS
                     src/generated_configuration.cc.temp
                     src/generated_configuration.h.temp
                     src/generated_enums.cc.temp
                     src/generated_enums.h.temp
                  COMMENT "Copying code for configuration class")

# Generate the flatbuffers data used by at least couchstore for storing the collection manifest
ADD_CUSTOM_COMMAND(OUTPUT
                     ${CMAKE_CURRENT_BINARY_DIR}/src/collections/kvstore_generated.h
                     ${CMAKE_CURRENT_BINARY_DIR}/src/collections/kvstore_flatbuffers_schema.cc
                  COMMAND
                     ${FLATC} -o ${CMAKE_CURRENT_BINARY_DIR}/src/collections/ --cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/collections/kvstore.fbs
                  COMMAND
                     ${CMAKE_COMMAND}
                     -DDIR1=${CMAKE_CURRENT_SOURCE_DIR}
                     -DDIR2=${CMAKE_CURRENT_BINARY_DIR}
                     -P ${CMAKE_CURRENT_SOURCE_DIR}/src/collections/generate_flatbuffers_raw_schema.cmake
                  DEPENDS
                    src/collections/kvstore.fbs
                    src/collections/kvstore.in
                  COMMENT "Generating flatbuffers kvstore data")

ADD_CUSTOM_COMMAND(OUTPUT
                     ${CMAKE_CURRENT_BINARY_DIR}/src/collections/events_generated.h
                  COMMAND
                     ${FLATC} -o ${CMAKE_CURRENT_BINARY_DIR}/src/collections/ --cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/collections/events.fbs
                  DEPENDS
                    src/collections/events.fbs
                  COMMENT "Generating flatbuffers - collection events")

ADD_CUSTOM_COMMAND(OUTPUT
                     ${CMAKE_CURRENT_BINARY_DIR}/src/collections/manifest_generated.h
                  COMMAND
                     ${FLATC} -o ${CMAKE_CURRENT_BINARY_DIR}/src/collections/ --cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/collections/manifest.fbs
                  DEPENDS
                    src/collections/manifest.fbs
                  COMMENT "Generating flatbuffers - collection manifest")


ADD_CUSTOM_COMMAND(OUTPUT
                     ${CMAKE_CURRENT_BINARY_DIR}/src/backup/backup_generated.h
                  COMMAND
                     ${FLATC} -I ${CMAKE_CURRENT_SOURCE_DIR}/src --keep-prefix -o ${CMAKE_CURRENT_BINARY_DIR}/src/backup/ --cpp ${CMAKE_CURRENT_SOURCE_DIR}/src/backup/backup.fbs
                  DEPENDS
                    src/backup/backup.fbs
                  COMMENT "Generating flatbuffers - backup")

# Custom build target to generate engine specific source files that we normally generate as part
# of a build.
add_custom_target(ep_generated_source_files
        DEPENDS
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h
            src/generated_configuration.cc
            src/generated_configuration.h
            src/generated_enums.cc
            src/generated_enums.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/backup/backup_generated.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/collections/kvstore_generated.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/collections/kvstore_flatbuffers_schema.cc
            ${CMAKE_CURRENT_BINARY_DIR}/src/collections/events_generated.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/collections/manifest_generated.h
            generate_audit_descriptors)

# Collections library for things required outside of ep-engine
ADD_LIBRARY(ep-engine_collections STATIC
        ${CMAKE_CURRENT_BINARY_DIR}/src/collections/kvstore_flatbuffers_schema.cc
        src/collections/collection_persisted_stats.cc)

SET_PROPERTY(TARGET ep-engine_collections PROPERTY POSITION_INDEPENDENT_CODE 1)

kv_enable_pch(ep-engine_collections)

target_link_libraries(ep-engine_collections PRIVATE platform)

TARGET_INCLUDE_DIRECTORIES(ep-engine_collections
                           PUBLIC
                           ${CMAKE_CURRENT_BINARY_DIR}/src/)

add_library(ep-engine_storage_common STATIC
            src/kvstore/storage_common/storage_common/local_doc_parser.cc
            src/kvstore/storage_common/storage_common/doc_key_encoder.cc)
SET_PROPERTY(TARGET ep-engine_storage_common PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(ep-engine_storage_common)
target_link_libraries(ep-engine_storage_common PRIVATE
        ep-engine_collections
        mcd_util
        ${FLATBUFFERS_LIBRARIES}
        Folly::headers)
target_include_directories(ep-engine_storage_common PUBLIC
                           src/kvstore/storage_common)

# Skip precompiled headers for the (tiny minority) of C source files.
# This is to avoid adding complexity to precompiled_headers.h to handle C & C++
# includes differently. In addition to skipping the precompiled headers we also
# need to avoid including them in a unity objects as the inclusion of the
# precompiled header in the unity object would cause a similar issue.
set_source_files_properties(src/crc32.c
                            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
                            PROPERTIES
                            SKIP_PRECOMPILE_HEADERS ON
                            SKIP_UNITY_BUILD_INCLUSION ON)

ADD_LIBRARY(ep STATIC
            src/access_scanner.cc
            src/array_histogram.cc
            src/backup/backup.cc
            src/bgfetcher.cc
            src/blob.cc
            src/bloomfilter.cc
            src/bucket_logger.cc
            src/bucket_quota_change_task.cc
            src/callbacks.cc
            src/checkpoint.cc
            src/checkpoint_config.cc
            src/checkpoint_cursor.cc
            src/checkpoint_manager.cc
            src/checkpoint_remover.cc
            src/collections/collections_types.cc
            src/collections/eraser_context.cc
            src/collections/flush.cc
            src/collections/flush_accounting.cc
            src/collections/kvstore.cc
            src/collections/manager.cc
            src/collections/manifest.cc
            src/collections/scan_context.cc
            src/collections/system_event_types.cc
            src/collections/vbucket_filter.cc
            src/collections/vbucket_manifest.cc
            src/collections/vbucket_manifest_entry.cc
            src/collections/vbucket_manifest_handles.cc
            src/collections/vbucket_manifest_scope_entry.cc
            src/configuration.cc
            src/conflict_resolution.cc
            src/conn_store.cc
            src/connhandler.cc
            src/connmap.cc
            src/crc32.c
            src/cross_bucket_visitor_adapter.cc
            src/dcp/active_stream.cc
            src/dcp/active_stream.h
            src/dcp/active_stream_checkpoint_processor_task.cc
            src/dcp/active_stream_checkpoint_processor_task.h
            src/dcp/backfill-manager.cc
            src/dcp/backfill.cc
            src/dcp/backfill_by_id_disk.cc
            src/dcp/backfill_by_seqno_disk.cc
            src/dcp/backfill_disk.cc
            src/dcp/backfill_disk_to_stream.cc
            src/dcp/backfill_memory.cc
            src/dcp/backfill_to_stream.cc
            src/dcp/cache_transfer_stream.cc
            src/dcp/consumer.cc
            src/dcp/dcp-types.cc
            src/dcp/dcpconnmap.cc
            src/dcp/flow-control-manager.cc
            src/dcp/flow-control.cc
            src/dcp/msg_producers_border_guard.cc
            src/dcp/passive_stream.cc
            src/dcp/passive_stream.h
            src/dcp/producer.cc
            src/dcp/response.cc
            src/dcp/stream.cc
            src/defragmenter.cc
            src/defragmenter_visitor.cc
            src/diskdockey.cc
            src/doc_pre_expiry.cc
            src/doc_pre_expiry.h
            src/dockey_validator.cc
            src/durability/active_durability_monitor.cc
            src/durability/dead_durability_monitor.cc
            src/durability/durability_completion_task.cc
            src/durability/durability_monitor.cc
            src/durability/durability_monitor_impl.cc
            src/durability/passive_durability_monitor.cc
            src/durability_timeout_task.cc
            src/encryption_key_provider.cc
            src/environment.cc
            src/ep_bucket.cc
            src/ep_engine.cc
            src/ep_engine_group.cc
            src/ep_engine_storage.cc
            src/ep_task.cc
            src/ep_time.cc
            src/ep_types.cc
            src/ep_vb.cc
            src/ephemeral_bucket.cc
            src/ephemeral_mem_recovery.cc
            src/ephemeral_tombstone_purger.cc
            src/ephemeral_vb.cc
            src/ephemeral_vb_count_visitor.cc
            src/error_handler.cc
            src/event_driven_timeout_task.cc
            src/eviction_ratios.cc
            src/eviction_utils.cc
            src/ext_meta_parser.cc
            src/failover-table.cc
            src/file_ops_tracker.cc
            src/flusher.cc
            src/getkeys.cc
            src/hash_table.cc
            src/hlc.cc
            src/htresizer.cc
            src/initial_mfu_task.cc
            src/item.cc
            src/item_compressor.cc
            src/item_compressor_visitor.cc
            src/item_eviction_strategy.cc
            src/item_freq_decayer.cc
            src/item_freq_decayer_visitor.cc
            src/item_pager.cc
            src/kv_bucket.cc
            src/kvshard.cc
            src/kvstore/couch-kvstore/couch-fs-stats.cc
            src/kvstore/couch-kvstore/couch-kvstore-config.cc
            src/kvstore/couch-kvstore/couch-kvstore-db-holder.cc
            src/kvstore/couch-kvstore/couch-kvstore-file-cache.cc
            src/kvstore/couch-kvstore/couch-kvstore.cc
            src/kvstore/couch-kvstore/vbucket_encryption_keys_manager.cc
            src/kvstore/couch-kvstore/vbucket_encryption_keys_manager.h
            src/kvstore/kvstore.cc
            src/kvstore/kvstore_config.cc
            src/kvstore/kvstore_iface.cc
            src/kvstore/nexus-kvstore/nexus-kvstore-config.cc
            src/kvstore/nexus-kvstore/nexus-kvstore.cc
            src/kvstore/persistence_callback.cc
            src/kvstore/rollback_callback.cc
            src/learning_age_and_mfu_based_eviction.cc
            src/linked_list.cc
            src/locked_vbucket_ptr.cc
            src/memory_tracker.cc
            src/mfu_only_item_eviction.cc
            src/mutation_log.cc
            src/mutation_log_entry.cc
            src/mutation_log_writer.cc
            src/mutation_log_writer.h
            src/objectregistry.cc
            src/paging_visitor.cc
            src/pid_controller.cc
            src/pre_link_document_context.cc
            src/pre_link_document_context.h
            src/probabilistic_counter.cc
            src/progress_tracker.cc
            src/quota_sharing_item_pager.cc
            src/range_lock_manager.cc
            src/range_scans/range_scan.cc
            src/range_scans/range_scan_callbacks.cc
            src/range_scans/range_scan_continue_task.cc
            src/range_scans/range_scan_create_task.cc
            src/range_scans/range_scan_owner.cc
            src/range_scans/range_scan_types.cc
            src/rollback_result.cc
            src/seqlist.cc
            src/seqno_persistence_notify_task.cc
            src/serialised_dockey.cc
            src/snapshots/cache.cc
            src/snapshots/cache.h
            src/snapshots/download_snapshot_controller.cc
            src/snapshots/download_snapshot_controller.h
            src/snapshots/download_snapshot_task.cc
            src/snapshots/download_snapshot_task.h
            src/snapshots/download_snapshot_task_listener.cc
            src/snapshots/download_snapshot_task_listener.h
            src/stats.cc
            src/stored-value.cc
            src/stored_value_factories.cc
            src/stored_value_factories.h
            src/string_utils.cc
            src/systemevent_factory.cc
            src/tasks.cc
            src/vb_adapters.cc
            src/vb_commit.cc
            src/vb_count_visitor.cc
            src/vb_notifiable_task.cc
            src/vb_ready_queue.cc
            src/vb_ready_queue.h
            src/vb_visitors.cc
            src/vbucket.cc
            src/vbucket_bgfetch_item.cc
            src/vbucket_loader.cc
            src/vbucket_loading_task.cc
            src/vbucket_state.cc
            src/vbucketdeletiontask.cc
            src/vbucketmap.cc
            src/warmup.cc
            src/work_sharding.cc
            ${CMAKE_CURRENT_BINARY_DIR}/src/backup/backup_generated.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/collections/events_generated.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/collections/kvstore_generated.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/collections/manifest_generated.h
            ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.cc
            ${CMAKE_CURRENT_BINARY_DIR}/src/generated_enums.cc
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
            ${MAGMA_KVSTORE_SOURCE})
SET_PROPERTY(TARGET ep PROPERTY POSITION_INDEPENDENT_CODE 1)
cb_enable_unity_build(ep)
add_sanitizers(ep)
target_link_libraries(ep
        PUBLIC
        hdrhistogram
        platform
        statistics
        spdlog::spdlog
        PRIVATE
        ${EP_STORAGE_LIBS}
        ${LIBEVENT_LIBRARIES}
        cbcompress
        engine_utilities
        ep-engine_collections
        mcbp
        mc_client_connection
        mcd_executor
        mcd_tracing
        mcd_util
        phosphor
        snapshot
        xattr)
kv_enable_pch(ep)
target_include_directories(ep SYSTEM PRIVATE ${SNAPPY_INCLUDE_DIR})

if (NOT WIN32)
  # Enable the more efficient ThreadLocalStorage model
  SET_TARGET_PROPERTIES(ep
                        PROPERTIES
                        COMPILE_FLAGS
                        "-ftls-model=initial-exec")
endif(NOT WIN32)

# Convenience function to define an ep_testsuite /ep_perfsuite -style
# test.
function(add_ep_testsuite name source)
  cb_add_test_executable(${name} ${source})
  kv_enable_pch(${name})
  target_link_libraries(${name}
          PRIVATE
          engine_testapp
          mock_dcp
          ep_testsuite_objs
          engine_testapp_dependencies
          platform_cb_malloc_arena)
  add_sanitizers(${name})
endfunction()

add_subdirectory(tests)

SET(MOCK_SOURCE
    tests/mock/mock_cache_transfer_stream.cc
    tests/mock/mock_couch_kvstore.cc
    tests/mock/mock_dcp_conn_map.cc
    tests/mock/mock_dcp_consumer.cc
    tests/mock/mock_dcp_producer.cc
    tests/mock/mock_ep_bucket.cc
    tests/mock/mock_ephemeral_bucket.cc
    tests/mock/mock_ephemeral_vb.cc
    tests/mock/mock_kvstore.cc
    tests/mock/mock_stream.cc
    tests/mock/mock_synchronous_ep_engine.cc
    tests/mock/mock_taskable.cc
    ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.h
    ${CMAKE_CURRENT_BINARY_DIR}/src/generated_enums.h)

if(EP_USE_MAGMA)
    list(APPEND MOCK_SOURCE
         tests/mock/mock_magma_kvstore.cc
         tests/mock/mock_magma_filesystem.cc)
endif()

add_library(ep_mocks EXCLUDE_FROM_ALL OBJECT ${MOCK_SOURCE})
set_property(TARGET ep_mocks PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(ep_mocks PUBLIC platform hdrhistogram spdlog::spdlog GTest::gmock)
add_sanitizers(ep_mocks)
cb_enable_unity_build(ep_mocks)
kv_enable_pch(ep_mocks)

# While ep_perfsuite is arguably a "test" and hence should live under
# tests/, we *don't* want optimizations disabled for it hence keep in
# this directory.
add_ep_testsuite(ep_perfsuite tests/ep_perfsuite.cc)

cb_add_test_executable(ep_engine_benchmarks
               benchmarks/access_scanner_bench.cc
               benchmarks/benchmark_memory_tracker.cc
               benchmarks/checkpoint_iterator_bench.cc
               benchmarks/dcp_producer_bench.cc
               benchmarks/defragmenter_bench.cc
               benchmarks/engine_fixture.cc
               benchmarks/ep_engine_benchmarks_main.cc
               benchmarks/executor_bench.cc
               benchmarks/hash_table_bench.cc
               benchmarks/item_bench.cc
               benchmarks/paging_visitor_bench.cc
               benchmarks/vbcb_adaptor_bench.cc
               benchmarks/item_compressor_bench.cc
               benchmarks/kvstore_bench.cc
               benchmarks/vb_ready_queue_bench.cc
               benchmarks/vbucket_bench.cc
               benchmarks/probabilistic_counter_bench.cc
               benchmarks/quota_sharing_bench.cc
               benchmarks/stats_bench.cc
               benchmarks/tracing_bench.cc
               ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_cookie.cc
               ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc
               ${Memcached_SOURCE_DIR}/engines/ep/tests/module_tests/test_helpers.cc)
kv_enable_pch(ep_engine_benchmarks)

TARGET_LINK_LIBRARIES(ep_engine_benchmarks
        PRIVATE
                ep
                mock_dcp
                ep_mocks
                couchstore_test_fileops
                benchmark::benchmark
                cluster_framework
                GTest::gtest
                GTest::gmock
                platform_cb_malloc_arena)
TARGET_INCLUDE_DIRECTORIES(ep_engine_benchmarks PUBLIC
                           tests
                           benchmarks
                           ${Couchstore_SOURCE_DIR})

add_sanitizers(ep_engine_benchmarks)
cb_enable_unity_build(ep_engine_benchmarks)

cb_add_test_executable(ep-engine_sizes src/sizes.cc)
kv_enable_pch(ep-engine_sizes)
target_link_libraries(ep-engine_sizes ep platform)
add_sanitizers(ep-engine_sizes)

INSTALL(FILES
        docs/stats.org
        DESTINATION share/doc/ep-engine)

ENABLE_CODE_COVERAGE_REPORT()

if(CB_ENABLE_HEADER_INCLUDE_CHECK)
    # Extra (non-default) target to compile all ep-engine headers (for
    # build profiling / checking for correct #includes).
    file(GLOB_RECURSE ep_headers
      RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
      CONFIGURE_DEPENDS
      *.h)
    # Tell CMAKE that configuration.h.cc is dependant on generated_configuration
    set_property(SOURCE ${CMAKE_CURRENT_BINARY_DIR}/src/configuration.h.cc
                APPEND PROPERTY OBJECT_DEPENDS
                ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.h
                ${CMAKE_CURRENT_BINARY_DIR}/src/generated_enums.h)

    # List of headers to ignore / not attempt to compile
    list(REMOVE_ITEM ep_headers
      src/config.cmake.h  # XXXX
      src/tasks.def.h  # X-Macro file; cannot be compiled by itself
    )
    if(NOT EP_USE_MAGMA)
      list(REMOVE_ITEM ep_headers
              src/kvstore/magma-kvstore/magma-kvstore.h
              src/kvstore/magma-kvstore/magma-kvstore_config.h)
    endif()

    add_header_object_library(NAME ep_headers_obj HEADERS ${ep_headers})
endif(CB_ENABLE_HEADER_INCLUDE_CHECK)
