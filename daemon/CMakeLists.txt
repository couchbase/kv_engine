add_library(memcached_daemon STATIC
            bucket_type.cc
            buckets.cc
            buckets.h
            cluster_config.cc
            cluster_config.h
            cmdline.cc
            cmdline.h
            cookie_trace_context.h
            client_cert_config.cc
            client_cert_config.h
            config_parse.cc
            config_parse.h
            connection.cc
            connection.h
            error_map_manager.cc
            error_map_manager.h
            nobucket_taskable.cc
            nobucket_taskable.h
            external_auth_manager_thread.cc
            external_auth_manager_thread.h
            server_socket.cc
            server_socket.h
            connections.cc
            connections.h
            cookie.cc
            cookie.h
            datatype_filter.cc
            datatype_filter.h
            debug_helpers.cc
            debug_helpers.h
            doc_pre_expiry.cc
            doc_pre_expiry.h
            enginemap.cc
            enginemap.h
            environment.cc
            front_end_thread.h
            get_authorization_task.cc
            get_authorization_task.h
            ioctl.cc
            ioctl.h
            libevent_locking.cc
            libevent_locking.h
            listening_port.h
            log_macros.h
            mc_time.cc
            mc_time.h
            mcaudit.cc
            mcaudit.h
            mcbp.cc
            mcbp.h
            mcbp_executors.cc
            mcbp_executors.h
            mcbp_privileges.cc
            mcbp_privileges.h
            mcbp_validators.cc
            mcbp_validators.h
            memcached.cc
            network_interface.cc
            network_interface.h
            network_interface_description.cc
            network_interface_description.h
            network_interface_manager.cc
            network_interface_manager.h
            opentelemetry.cc
            opentelemetry.h
            protocol/mcbp/adjust_timeofday_executor.cc
            protocol/mcbp/appendprepend_context.cc
            protocol/mcbp/appendprepend_context.h
            protocol/mcbp/arithmetic_context.cc
            protocol/mcbp/arithmetic_context.h
            protocol/mcbp/audit_configure_context.cc
            protocol/mcbp/audit_configure_context.h
            protocol/mcbp/cluster_config_executor.cc
            protocol/mcbp/collections_get_collection_id_executor.cc
            protocol/mcbp/collections_get_manifest_executor.cc
            protocol/mcbp/collections_get_scope_id_executor.cc
            protocol/mcbp/collections_set_manifest_executor.cc
            protocol/mcbp/command_context.h
            protocol/mcbp/create_remove_bucket_command_context.cc
            protocol/mcbp/create_remove_bucket_command_context.h
            protocol/mcbp/dcp_abort_executor.cc
            protocol/mcbp/dcp_add_failover_log.cc
            protocol/mcbp/dcp_add_failover_log.h
            protocol/mcbp/dcp_add_stream_executor.cc
            protocol/mcbp/dcp_buffer_acknowledgement_executor.cc
            protocol/mcbp/dcp_close_stream_executor.cc
            protocol/mcbp/dcp_commit_executor.cc
            protocol/mcbp/dcp_control_executor.cc
            protocol/mcbp/dcp_deletion.cc
            protocol/mcbp/dcp_deletion.h
            protocol/mcbp/dcp_expiration.cc
            protocol/mcbp/dcp_expiration.h
            protocol/mcbp/dcp_get_failover_log_executor.cc
            protocol/mcbp/dcp_mutation.cc
            protocol/mcbp/dcp_mutation.h
            protocol/mcbp/dcp_noop_executor.cc
            protocol/mcbp/dcp_open_executor.cc
            protocol/mcbp/dcp_prepare_executor.cc
            protocol/mcbp/dcp_seqno_acknowledged_executor.cc
            protocol/mcbp/dcp_set_vbucket_state_executor.cc
            protocol/mcbp/dcp_snapshot_marker_executor.cc
            protocol/mcbp/dcp_stream_end_executor.cc
            protocol/mcbp/dcp_stream_req_executor.cc
            protocol/mcbp/dcp_system_event_executor.cc
            protocol/mcbp/dcp_system_event_executor.h
            protocol/mcbp/drop_privilege_executor.cc
            protocol/mcbp/engine_wrapper.cc
            protocol/mcbp/engine_wrapper.h
            protocol/mcbp/executors.h
            protocol/mcbp/executors.cc
            protocol/mcbp/flush_command_context.cc
            protocol/mcbp/flush_command_context.h
            protocol/mcbp/gat_context.cc
            protocol/mcbp/gat_context.h
            protocol/mcbp/get_cmd_timer_executor.cc
            protocol/mcbp/get_context.cc
            protocol/mcbp/get_context.h
            protocol/mcbp/get_locked_context.cc
            protocol/mcbp/get_locked_context.h
            protocol/mcbp/get_meta_context.cc
            protocol/mcbp/get_meta_context.h
            protocol/mcbp/hello_packet_executor.cc
            protocol/mcbp/ifconfig_context.cc
            protocol/mcbp/ifconfig_context.h
            protocol/mcbp/list_bucket_executor.cc
            protocol/mcbp/mutation_context.cc
            protocol/mcbp/mutation_context.h
            protocol/mcbp/range_scan_cancel_executor.cc
            protocol/mcbp/range_scan_continue_executor.cc
            protocol/mcbp/range_scan_create_executor.cc
            protocol/mcbp/rbac_reload_command_context.cc
            protocol/mcbp/rbac_reload_command_context.h
            protocol/mcbp/remove_context.cc
            protocol/mcbp/remove_context.h
            protocol/mcbp/sasl_auth_command_context.cc
            protocol/mcbp/sasl_auth_command_context.h
            protocol/mcbp/sasl_start_command_context.cc
            protocol/mcbp/sasl_start_command_context.h
            protocol/mcbp/sasl_step_command_context.cc
            protocol/mcbp/sasl_step_command_context.h
            protocol/mcbp/sasl_refresh_command_context.cc
            protocol/mcbp/sasl_refresh_command_context.h
            protocol/mcbp/select_bucket_executor.cc
            protocol/mcbp/session_validated_command_context.cc
            protocol/mcbp/session_validated_command_context.h
            protocol/mcbp/settings_reload_command_context.cc
            protocol/mcbp/settings_reload_command_context.h
            protocol/mcbp/stats_context.cc
            protocol/mcbp/stats_context.h
            protocol/mcbp/steppable_command_context.cc
            protocol/mcbp/steppable_command_context.h
            protocol/mcbp/unlock_context.cc
            protocol/mcbp/unlock_context.h
            protocol/mcbp/utilities.h
            sendbuffer.cc
            sendbuffer.h
            server_api.cc
            session_cas.cc
            session_cas.h
            settings.cc
            settings.h
            sloppy_compute_unit_gauge.cc
            sloppy_compute_unit_gauge.h
            ssl_utils.cc
            ssl_utils.h
            start_sasl_auth_task.cc
            start_sasl_auth_task.h
            stats.cc
            stats.h
            stats_tasks.cc
            stats_tasks.h
            stdin_check.cc
            subdocument.cc
            subdocument.h
            subdocument_context.h
            subdocument_context.cc
            subdocument_traits.cc
            subdocument_traits.h
            subdocument_validators.cc
            subdocument_validators.h
            thread.cc
            timing_histogram.cc
            timing_histogram.h
            timing_interval.cc
            timing_interval.h
            timings.cc
            timings.h
            stat_timings.cc
            stat_timings.h
            tls_configuration.cc
            tls_configuration.h
            tracing.cc
            tracing.h)

if (NUMA_FOUND)
    target_include_directories(memcached_daemon
                               SYSTEM PRIVATE ${NUMA_INCLUDE_DIR})
    target_link_libraries(memcached_daemon PRIVATE ${NUMA_LIBRARIES})
endif()

target_include_directories(memcached_daemon PRIVATE ${Memcached_BINARY_DIR})
cb_enable_unity_build(memcached_daemon)

ADD_DEPENDENCIES(memcached_daemon generate_audit_descriptors)

target_link_libraries(memcached_daemon
        PUBLIC
                      mcd_util
                      memcached_logger
                      platform
                      subjson
                      hdrhistogram
        PRIVATE
                      auditd
                      cbsocket
                      crash_engine
                      default_engine
                      ep
                      ewouldblock_engine
                      mcd_executor
                      mcd_tracing
                      mcbp
                      nobucket
                      cbsasl
                      cbcompress
                      engine_utilities
                      gsl::gsl-lite
                      platform_cb_malloc_arena
                      json_validator
                      memcached_rbac
                      phosphor
                      xattr
                      ${SNAPPY_LIBRARIES}
                      ${MALLOC_LIBRARIES}
                      ${LIBEVENT_LIBRARIES}
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS}
                      ${MEMCACHED_EXTRA_LIBS})
add_sanitizers(memcached_daemon)
kv_enable_pch(memcached_daemon)

ADD_EXECUTABLE(memcached main.cc)
target_link_libraries(memcached PRIVATE memcached_daemon)
add_sanitizers(memcached)
kv_enable_pch(memcached)
# Allow 'memcached' to export symbols - i.e. symbols marked as
# MEMCACHED_PUBLIC_API / MEMCACHED_PUBLIC_CLASS should marked as externally
# visible in the symbol table.
# We don't strictly _need_ this setting, memcached is now statically linked
# and hence no symbols need to be visible externally for the product, however
# we do still mark a test function (recursive_crash_function in crash_engine)
# As MEMCACHED_PUBLIC_API to verify Breakpad is working correctly and it's
# valuble to still be able to see that symbol.
set_target_properties(memcached PROPERTIES ENABLE_EXPORTS TRUE)

INSTALL(TARGETS memcached
        RUNTIME DESTINATION bin)

#
# Unit tests
#
cb_add_test_executable(memcached_unit_tests
                       bucket_validator_test.cc
                       buckets_test.cc
                       client_cert_config_test.cc
                       connection_unit_tests.cc
                       datatype_filter_test.cc
                       error_map_manager_test.cc
                       doc_pre_expiry_test.cc
                       function_chain_test.cc
                       json_validator_test.cc
                       memcached_unit_test_main.cc
                       mc_time_test.cc
                       network_interface_description_test.cc
                       settings_test.cc
                       sloppy_compute_unit_gauge_test.cc
                       ssl_utils_test.cc
                       tls_configuration_test.cc)
add_dependencies(memcached_unit_tests Generate_kv_engine_X509_Test_Certificates)
cb_enable_unity_build(memcached_unit_tests)
add_sanitizers(memcached_unit_tests)
target_link_libraries(memcached_unit_tests PRIVATE
                      memcached_daemon
                      GTest::gtest)
add_test(NAME memcached_unit_tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_unit_tests)
kv_enable_pch(memcached_unit_tests)
