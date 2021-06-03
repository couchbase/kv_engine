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
            mcbp_topkeys.cc
            mcbp_topkeys.h
            mcbp_validators.cc
            mcbp_validators.h
            memcached.cc
            network_interface.cc
            network_interface.h
            network_interface_manager.cc
            network_interface_manager.h
            opentelemetry.cc
            opentelemetry.h
            parent_monitor.cc
            parent_monitor.h
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
            protocol/mcbp/list_bucket_executor.cc
            protocol/mcbp/mutation_context.cc
            protocol/mcbp/mutation_context.h
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
            protocol/mcbp/stats_context.cc
            protocol/mcbp/stats_context.h
            protocol/mcbp/steppable_command_context.cc
            protocol/mcbp/steppable_command_context.h
            protocol/mcbp/unlock_context.cc
            protocol/mcbp/unlock_context.h
            protocol/mcbp/utilities.h
            runtime.cc
            runtime.h
            sendbuffer.cc
            sendbuffer.h
            server_api.cc
            session_cas.cc
            session_cas.h
            settings.cc
            settings.h
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
            topkeys.cc
            topkeys.h
            tracing.cc
            tracing.h)

if (NUMA_FOUND)
    target_include_directories(memcached_daemon
                               SYSTEM PRIVATE ${NUMA_INCLUDE_DIR})
    target_link_libraries(memcached_daemon ${NUMA_LIBRARIES})
endif()

target_include_directories(memcached_daemon PRIVATE ${Memcached_BINARY_DIR})
cb_enable_unity_build(memcached_daemon)

ADD_DEPENDENCIES(memcached_daemon generate_audit_descriptors)

TARGET_LINK_LIBRARIES(memcached_daemon
                      auditd
                      cbsocket
                      crash_engine
                      default_engine
                      ep
                      ewouldblock_engine
                      mcd_executor
                      mcd_util
                      mcd_tracing
                      mcbp
                      memcached_logger
                      nobucket
                      cbsasl
                      cbcompress
                      engine_utilities
                      gsl_lite
                      platform
                      platform_cb_malloc_arena
                      JSON_checker
                      memcached_rbac
                      subjson
                      phosphor
                      xattr
                      ${SNAPPY_LIBRARIES}
                      ${MALLOC_LIBRARIES}
                      ${LIBEVENT_LIBRARIES}
                      ${OPENSSL_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS}
                      ${MEMCACHED_EXTRA_LIBS})
add_sanitizers(memcached_daemon)

ADD_EXECUTABLE(memcached main.cc)
TARGET_LINK_LIBRARIES(memcached memcached_daemon)
add_sanitizers(memcached)

INSTALL(TARGETS memcached
        RUNTIME DESTINATION bin)

#
# Unit tests
#
if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(client_cert_config_test
                   client_cert_config_test.cc
                   client_cert_config.cc
                   client_cert_config.h)
    target_link_libraries(client_cert_config_test PRIVATE
            gtest
            gtest_main
            platform
            mcd_util
            ${OPENSSL_LIBRARIES})
    add_test(NAME client_cert_config_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND client_cert_config_test)
    add_sanitizers(client_cert_config_test)

    add_executable(memcached_unit_tests
                   bucket_validator_test.cc
                   connection_unit_tests.cc
                   datatype_filter_test.cc
                   doc_pre_expiry_test.cc
                   function_chain_test.cc
                   mc_time_test.cc
                   settings_test.cc
                   ssl_utils_test.cc)
    add_sanitizers(memcached_unit_tests)
    target_link_libraries(memcached_unit_tests
                          memcached_daemon
                          gtest
                          gtest_main)
    add_test(NAME memcached_unit_tests
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached_unit_tests)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
