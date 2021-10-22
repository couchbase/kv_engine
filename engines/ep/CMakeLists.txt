PROJECT(EventuallyPersistentEngine LANGUAGES CXX)

INCLUDE(CheckFunctionExists)
INCLUDE(CheckLibraryExists)
INCLUDE(CheckSymbolExists)
INCLUDE(CheckTypeSize)
INCLUDE(CMakeDependentOption)
INCLUDE(CTest)

CMAKE_DEPENDENT_OPTION(EP_USE_MAGMA "Enable support for Magma" ON
    "MAGMA_INCLUDE_DIR" OFF)

CMAKE_DEPENDENT_OPTION(EP_USE_ROCKSDB "Enable support for RocksDB" ON
        "ROCKSDB_INCLUDE_DIR;ROCKSDB_LIBRARIES" OFF)

INCLUDE_DIRECTORIES(BEFORE ${CMAKE_INSTALL_PREFIX}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/src
                           ${CMAKE_CURRENT_BINARY_DIR}/src
                           ${BOOST_INCLUDE_DIR}
                           ${SNAPPY_INCLUDE_DIR}
                           ${BOOST_INCLUDE_DIR}
                           ${FLATBUFFERS_INCLUDE_DIR}
                           ${Platform_SOURCE_DIR}/include
                           ${Platform_SOURCE_DIR}/external
                           ${Memcached_SOURCE_DIR}
                           ${Memcached_SOURCE_DIR}/utilities
                           ${Memcached_SOURCE_DIR}/tracing
                           ${Memcached_SOURCE_DIR}/include
                           ${Couchstore_SOURCE_DIR}/include
                           ${Couchstore_BINARY_DIR}/include
                           ${phosphor_SOURCE_DIR}/include
                           ${CMAKE_CURRENT_BINARY_DIR}
        )

# Mark third-party libraries as 'system' so we skip any warnings they
# generate.
INCLUDE_DIRECTORIES(BEFORE SYSTEM ${hdr_histogram_SOURCE_DIR}/src)

INCLUDE_DIRECTORIES(AFTER ${Memcached_BINARY_DIR}/include)

SET(EP_STORAGE_LIBS couchstore_static ep-engine_storage_common)

IF (EP_USE_ROCKSDB)
    INCLUDE_DIRECTORIES(AFTER SYSTEM ${ROCKSDB_INCLUDE_DIR})
    SET(ROCKSDB_KVSTORE_SOURCE src/kvstore/rocksdb-kvstore/rocksdb-kvstore.cc
            src/kvstore/rocksdb-kvstore/rocksdb-kvstore_config.cc)
    LIST(APPEND EP_STORAGE_LIBS ${ROCKSDB_LIBRARIES})
    ADD_DEFINITIONS(-DEP_USE_ROCKSDB=1)
    MESSAGE(STATUS "ep-engine: Using RocksDB")
ENDIF (EP_USE_ROCKSDB)

IF (EP_USE_MAGMA)
    INCLUDE_DIRECTORIES(AFTER ${MAGMA_INCLUDE_DIR})
    LIST(APPEND EP_STORAGE_LIBS magma)
    SET(MAGMA_KVSTORE_SOURCE
            src/kvstore/magma-kvstore/magma-kvstore.cc
            src/kvstore/magma-kvstore/magma-kvstore_config.cc
            src/kvstore/magma-kvstore/magma-memory-tracking-proxy.cc)
    ADD_DEFINITIONS(-DEP_USE_MAGMA=1)
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

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${CMAKE_SOURCE_DIR}/platform/include/win32)
ENDIF (WIN32)

add_executable(gencode tools/gencode.cc)
target_link_libraries(gencode PRIVATE nlohmann_json::nlohmann_json)
kv_enable_pch(gencode)
add_executable(genconfig tools/genconfig.cc)
target_link_libraries(genconfig PRIVATE nlohmann_json::nlohmann_json)
kv_enable_pch(genconfig)

add_executable(pid_runner tools/pid_runner.cc $<TARGET_OBJECTS:ep_objs>)

if (WIN32)
    # windows need getopt
    target_link_libraries(gencode PRIVATE platform)
    target_link_libraries(genconfig PRIVATE platform)
endif()
add_sanitizers(genconfig)

add_subdirectory(tools/couchfile_upgrade)
include_directories(tools/couchfile_upgrade)

cb_add_test_executable(kvstore_gen tools/kvstore_gen.cc
        ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_cookie.cc
        ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc
        ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
        $<TARGET_OBJECTS:ep_objs>)
kv_enable_pch(kvstore_gen)

target_link_libraries(kvstore_gen PRIVATE
        JSON_checker
        cbcompress
        engine_utilities
        ep-engine_collections
        hdr_histogram_static
        mcd_executor
        mcbp
        mcd_time
        mcd_tracing
        mcd_util
        platform
        phosphor
        statistics
        xattr
        ${EP_STORAGE_LIBS}
        ${LIBEVENT_LIBRARIES}
)

target_link_libraries(pid_runner
        ep-engine_collections
        mcd_executor
        mcbp
        mcd_time
        mcd_tracing
        xattr
        ${EP_STORAGE_LIBS})

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
                     src/stats_config.def.h.temp
                  COMMAND
                     ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/src
                  COMMAND
                     genconfig ${CMAKE_CURRENT_SOURCE_DIR}/configuration.json src/generated_configuration.h.temp src/generated_configuration.cc.temp src/stats_config.def.h.temp
                  DEPENDS
                        configuration.json
                        genconfig
                  COMMENT "Generating code for configuration class")

ADD_CUSTOM_COMMAND(OUTPUT
                     src/generated_configuration.cc
                     src/generated_configuration.h
                     src/stats_config.def.h
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_configuration.cc.temp src/generated_configuration.cc
                  COMMAND
                     ${CMAKE_COMMAND} -E copy_if_different src/generated_configuration.h.temp src/generated_configuration.h
                  COMMAND
                    ${CMAKE_COMMAND} -E copy_if_different src/stats_config.def.h.temp src/stats_config.def.h
                  DEPENDS
                     src/generated_configuration.cc.temp
                     src/generated_configuration.h.temp
                     src/stats_config.def.h.temp
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

# Custom build target to generate source files that we normally generate as part
# of a build.
add_custom_target(generated_source_files
        DEPENDS
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.h
            src/generated_configuration.cc
            src/generated_configuration.h
            src/stats_config.def.h
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
            src/kvstore/storage_common/storage_common/local_doc_parser.cc)
SET_PROPERTY(TARGET ep-engine_storage_common PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(ep-engine_storage_common)
target_link_libraries(ep-engine_storage_common PRIVATE
        ep-engine_collections
        ${FLATBUFFERS_LIBRARIES}
        Folly::headers)
target_include_directories(ep-engine_storage_common PUBLIC
                           src/kvstore/storage_common)

SET(COUCH_KVSTORE_SOURCE src/kvstore/couch-kvstore/couch-fs-stats.cc
        src/kvstore/couch-kvstore/couch-kvstore.cc
        src/kvstore/couch-kvstore/couch-kvstore-config.cc
        src/kvstore/couch-kvstore/couch-kvstore-db-holder.cc
        src/kvstore/couch-kvstore/couch-kvstore-file-cache.cc)
SET(OBJECTREGISTRY_SOURCE src/objectregistry.cc)
SET(CONFIG_SOURCE src/configuration.cc
  ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.cc)

SET(COLLECTIONS_SOURCE src/collections/collections_types.cc
                       src/collections/eraser_context.cc
                       src/collections/flush.cc
                       src/collections/flush_accounting.cc
                       src/collections/kvstore.cc
                       src/collections/manager.cc
                       src/collections/manifest.cc
                       src/collections/persist_manifest_task.cc
                       ${CMAKE_CURRENT_BINARY_DIR}/src/collections/events_generated.h
                       ${CMAKE_CURRENT_BINARY_DIR}/src/collections/kvstore_generated.h
                       ${CMAKE_CURRENT_BINARY_DIR}/src/collections/manifest_generated.h
                       src/collections/scan_context.cc
                       src/collections/system_event_types.cc
                       src/collections/vbucket_filter.cc
                       src/collections/vbucket_manifest.cc
                       src/collections/vbucket_manifest_entry.cc
                       src/collections/vbucket_manifest_handles.cc
                       src/collections/vbucket_manifest_scope_entry.cc)

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

ADD_LIBRARY(ep_objs OBJECT
            src/access_scanner.cc
            src/bgfetcher.cc
            src/blob.cc
            src/bloomfilter.cc
            src/bucket_logger.cc
            src/callbacks.cc
            src/checkpoint.cc
            src/checkpoint_config.cc
            src/checkpoint_manager.cc
            src/checkpoint_remover.cc
            src/conflict_resolution.cc
            src/conn_notifier.cc
            src/connhandler.cc
            src/connmap.cc
            src/conn_store.cc
            src/crc32.c
            src/dcp/active_stream.cc
            src/dcp/active_stream.h
            src/dcp/active_stream_checkpoint_processor_task.cc
            src/dcp/active_stream_checkpoint_processor_task.h
            src/dcp/backfill.cc
            src/dcp/backfill_by_id_disk.cc
            src/dcp/backfill_by_seqno_disk.cc
            src/dcp/backfill_disk.cc
            src/dcp/backfill-manager.cc
            src/dcp/backfill_memory.cc
            src/dcp/consumer.cc
            src/dcp/dcp-types.h
            src/dcp/dcpconnmap.cc
            src/dcp/flow-control.cc
            src/dcp/flow-control-manager.cc
            src/dcp/msg_producers_border_guard.cc
            src/dcp/passive_stream.cc
            src/dcp/passive_stream.h
            src/dcp/producer.cc
            src/vb_ready_queue.cc
            src/vb_ready_queue.h
            src/dcp/response.cc
            src/dcp/stream.cc
            src/defragmenter.cc
            src/defragmenter_visitor.cc
            src/diskdockey.cc
            src/dockey_validator.cc
            src/durability/active_durability_monitor.cc
            src/durability/durability_completion_task.cc
            src/durability/durability_monitor.cc
            src/durability/durability_monitor_impl.cc
            src/durability/passive_durability_monitor.cc
            src/durability_timeout_task.cc
            src/ep_bucket.cc
            src/ep_vb.cc
            src/ep_engine.cc
            src/ep_time.cc
            src/ep_types.cc
            src/ephemeral_bucket.cc
            src/ephemeral_tombstone_purger.cc
            src/ephemeral_vb.cc
            src/ephemeral_vb_count_visitor.cc
            src/error_handler.cc
            src/environment.cc
            src/ext_meta_parser.cc
            src/failover-table.cc
            src/flusher.cc
            src/getkeys.cc
            src/hash_table.cc
            src/hlc.cc
            src/htresizer.cc
            src/item.cc
            src/item_compressor.cc
            src/item_compressor_visitor.cc
            src/item_eviction.cc
            src/item_freq_decayer.cc
            src/item_freq_decayer_visitor.cc
            src/item_pager.cc
        src/kvstore/nexus-kvstore/nexus-kvstore.cc
        src/kvstore/nexus-kvstore/nexus-kvstore-config.cc
        src/kvstore/kvstore.cc
        src/kvstore/kvstore_config.cc
            src/kv_bucket.cc
            src/kvshard.cc
            src/mutation_log.cc
            src/mutation_log_entry.cc
            src/paging_visitor.cc
        src/kvstore/persistence_callback.cc
            src/pid_controller.cc
            src/pre_link_document_context.cc
            src/pre_link_document_context.h
            src/progress_tracker.cc
            src/replicationthrottle.cc
            src/linked_list.cc
            src/range_lock_manager.cc
            src/rollback_result.cc
            src/server_document_iface_border_guard.cc
            src/server_document_iface_border_guard.h
            src/seqlist.cc
            src/stats.cc
            src/string_utils.cc
            src/storeddockey.cc
            src/stored-value.cc
            src/stored_value_factories.cc
            src/stored_value_factories.h
            src/systemevent_factory.cc
            src/tasks.cc
            src/vb_commit.cc
            src/vb_count_visitor.cc
            src/vb_notifiable_task.cc
            src/vb_visitors.cc
            src/vbucket.cc
            src/vbucket_bgfetch_item.cc
            src/vbucket_state.cc
            src/vbucketmap.cc
            src/vbucketdeletiontask.cc
            src/warmup.cc
            ${OBJECTREGISTRY_SOURCE}
            ${CMAKE_CURRENT_BINARY_DIR}/src/stats-info.c
            ${CONFIG_SOURCE}
            ${COUCH_KVSTORE_SOURCE}
            ${ROCKSDB_KVSTORE_SOURCE}
            ${MAGMA_KVSTORE_SOURCE}
            ${COLLECTIONS_SOURCE})
SET_PROPERTY(TARGET ep_objs PROPERTY POSITION_INDEPENDENT_CODE 1)
cb_enable_unity_build(ep_objs)

# MB-46833: Suspect a gcc bug where consumer.cc + producer.cc together fails.
# Here we will build producer.cc alone and avoid the issue only on gcc. Same is
# seen for warmup.cc.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
set_source_files_properties(src/dcp/producer.cc
                            src/warmup.cc
                            PROPERTIES SKIP_UNITY_BUILD_INCLUSION 1)
endif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")

add_sanitizers(ep_objs)
target_link_libraries(ep_objs PUBLIC mcd_executor platform spdlog::spdlog ${EP_STORAGE_LIBS})
kv_enable_pch(ep_objs)

ADD_LIBRARY(ep STATIC $<TARGET_OBJECTS:ep_objs>)
kv_enable_pch(ep)

if (NOT WIN32)
  # Enable the more efficient ThreadLocalStorage model
  SET_TARGET_PROPERTIES(ep_objs
                        PROPERTIES
                        COMPILE_FLAGS
                        "-ftls-model=initial-exec")
endif(NOT WIN32)

target_link_libraries(ep
        PRIVATE
                ${EP_STORAGE_LIBS}
                ${LIBEVENT_LIBRARIES}
                JSON_checker
                cbcompress
                engine_utilities
                ep-engine_collections
                hdr_histogram_static
                mcbp
                mcd_executor
                mcd_time
                mcd_tracing
                mcd_util
                phosphor
                platform
                statistics
                xattr)
add_sanitizers(ep)

# Convenience function to define an ep_testsuite /ep_perfsuite -style
# test.
function(add_ep_testsuite name source)
  cb_add_test_executable(${name}
    ${source}
    $<TARGET_OBJECTS:ep_testsuite_objs>
    $<TARGET_OBJECTS:mock_dcp>
    $<TARGET_OBJECTS:engine_testapp>
    )
  kv_enable_pch(${name})

  target_link_libraries(${name}
    PRIVATE
        ${LIBEVENT_LIBRARIES}
        JSON_checker
        cbcompress
        engine_testapp_dependencies
        engine_utilities
        mcbp
        mcd_executor
        mcd_util
        platform
        platform_cb_malloc_arena
        xattr
    )
  add_sanitizers(${name})
endfunction()

add_subdirectory(tests)

SET(MOCK_SOURCE
    tests/mock/mock_dcp_conn_map.cc
    tests/mock/mock_dcp_consumer.cc
    tests/mock/mock_dcp_producer.cc
    tests/mock/mock_ep_bucket.cc
    tests/mock/mock_ephemeral_bucket.cc
    tests/mock/mock_ephemeral_vb.cc
    tests/mock/mock_kvstore.cc
    tests/mock/mock_replicationthrottle.cc
    tests/mock/mock_stream.cc
    tests/mock/mock_synchronous_ep_engine.cc
    tests/mock/mock_taskable.cc
    ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.h)

if(EP_USE_MAGMA)
    list(APPEND MOCK_SOURCE tests/mock/mock_magma_kvstore.cc)
endif()

add_library(ep_mocks OBJECT ${MOCK_SOURCE})
set_property(TARGET ep_mocks PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(ep_mocks PUBLIC platform spdlog::spdlog GTest::gmock)
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
               benchmarks/item_pager_bench.cc
               benchmarks/item_compressor_bench.cc
               benchmarks/kvstore_bench.cc
               benchmarks/vb_ready_queue_bench.cc
               benchmarks/vbucket_bench.cc
               benchmarks/probabilistic_counter_bench.cc
               benchmarks/stats_bench.cc
               benchmarks/tracing_bench.cc
               $<TARGET_OBJECTS:mock_dcp>
               $<TARGET_OBJECTS:ep_objs>
               $<TARGET_OBJECTS:ep_mocks>
               $<TARGET_OBJECTS:couchstore_test_fileops>
               ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_cookie.cc
               ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc
               ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
               ${Memcached_SOURCE_DIR}/engines/ep/tests/module_tests/test_helpers.cc)
kv_enable_pch(ep_engine_benchmarks)

TARGET_LINK_LIBRARIES(ep_engine_benchmarks
        PRIVATE
                benchmark::benchmark
                cbcompress
                engine_utilities
                ep-engine_collections
                mcd_executor
                GTest::gtest
                GTest::gmock
                hdr_histogram_static
                JSON_checker
                mcbp
                mcd_util
                mcd_time
                mcd_tracing
                memcached_logger
                phosphor
                platform
                platform_cb_malloc_arena
                statistics
                xattr
                ${MALLOC_LIBRARIES}
                ${EP_STORAGE_LIBS}
                ${LIBEVENT_LIBRARIES})
TARGET_INCLUDE_DIRECTORIES(ep_engine_benchmarks PUBLIC
                           tests
                           benchmarks
                           ${Couchstore_SOURCE_DIR})

add_sanitizers(ep_engine_benchmarks)
cb_enable_unity_build(ep_engine_benchmarks)

cb_add_test_executable(ep-engine_sizes src/sizes.cc
               $<TARGET_OBJECTS:ep_objs>)
kv_enable_pch(ep-engine_sizes)
target_link_libraries(ep-engine_sizes
        PRIVATE
                ${EP_STORAGE_LIBS}
                ${LIBEVENT_LIBRARIES}
                JSON_checker
                cbcompress
                engine_utilities
                ep-engine_collections
                hdr_histogram_static
                mcbp
                mcd_executor
                mcd_time
                mcd_tracing
                mcd_util
                phosphor
                platform
                statistics
                xattr)
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
                ${CMAKE_CURRENT_BINARY_DIR}/src/generated_configuration.h)

    # List of headers to ignore / not attempt to compile
    list(REMOVE_ITEM ep_headers
      src/bucket_logger_impl.h # Included via bucket_logger.h
      src/config.cmake.h  # XXXX
      src/tasks.def.h  # X-Macro file; cannot be compiled by itself
      src/threadlocal_posix.h  # POSIX specific code; included via threadlocal.h
      src/threadlocal_win32.h  # Win32 specific code; included via threadlocal.h
    )
    if(NOT EP_USE_MAGMA)
      list(REMOVE_ITEM ep_headers
              src/kvstore/magma-kvstore/magma-kvstore.h
              src/kvstore/magma-kvstore/magma-kvstore_config.h)
    endif()

    add_header_object_library(NAME ep_headers_obj HEADERS ${ep_headers})
endif(CB_ENABLE_HEADER_INCLUDE_CHECK)
