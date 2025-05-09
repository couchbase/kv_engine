add_subdirectory(protocol)

add_library(memcached_daemon STATIC
            bucket_type.cc
            bucket_destroyer.cc
            bucket_destroyer.h
            buckets.cc
            buckets.h
            client_cert_config.cc
            client_cert_config.h
            cluster_config.cc
            cluster_config.h
            cmdline.cc
            cmdline.h
            concurrency_semaphores.cc
            concurrency_semaphores.h
            config_parse.cc
            config_parse.h
            connection.cc
            connection.h
            connection_libevent.cc
            connection_libevent.h
            cookie.cc
            cookie.h
            cookie_trace_context.h
            datatype_filter.cc
            datatype_filter.h
            enginemap.cc
            enginemap.h
            environment.cc
            error_map_manager.cc
            error_map_manager.h
            external_auth_manager_thread.cc
            external_auth_manager_thread.h
            front_end_thread.cc
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
            network_interface_manager_thread.cc
            network_interface_manager_thread.h
            nobucket_taskable.cc
            nobucket_taskable.h
            resource_allocation_domain.cc
            resource_allocation_domain.h
            sasl_auth_task.cc
            sasl_auth_task.h
            sdk_connection_manager.cc
            sdk_connection_manager.h
            sendbuffer.cc
            sendbuffer.h
            server_api.cc
            server_socket.cc
            server_socket.h
            settings.cc
            settings.h
            sloppy_gauge.cc
            sloppy_gauge.h
            ssl_utils.cc
            ssl_utils.h
            stat_timings.cc
            stat_timings.h
            stats.cc
            stats.h
            stats_tasks.cc
            stats_tasks.h
            stdin_check.cc
            subdocument.cc
            subdocument.h
            subdocument_context.cc
            subdocument_context.h
            subdocument_traits.cc
            subdocument_traits.h
            subdocument_validators.cc
            subdocument_validators.h
            timing_histogram.cc
            timing_histogram.h
            timing_interval.cc
            timing_interval.h
            timings.cc
            timings.h
            tls_configuration.cc
            tls_configuration.h
            tracing.cc
            tracing.h
            yielding_limited_concurrency_task.cc
            yielding_task.cc
        )

if (NUMA_FOUND)
    target_include_directories(memcached_daemon
                               SYSTEM PRIVATE ${NUMA_INCLUDE_DIR})
    target_link_libraries(memcached_daemon PRIVATE ${NUMA_LIBRARIES})
endif()

target_include_directories(memcached_daemon PRIVATE ${Memcached_BINARY_DIR})
cb_enable_unity_build(memcached_daemon)

add_dependencies(memcached_daemon
                 generate_audit_descriptors
                 generate_serverless_configuration)

target_link_libraries(memcached_daemon
        PUBLIC
                      mcd_util
                      memcached_logger
                      platform
                      OpenSSL::SSL
                      subjson
                      hdrhistogram
        PRIVATE
                      memcached_daemon_mcbp
                      auditd
                      cbsocket
                      crash_engine
                      ep
                      ewouldblock_engine
                      folly_io_callbacks
                      mcd_dek
                      mcd_executor
                      mcd_tracing
                      mcbp
                      nobucket
                      cbsasl
                      cbcompress
                      engine_utilities
                      gsl::gsl-lite
                      json_validator
                      memcached_rbac
                      phosphor
                      xattr
                      sigar_cpp
                      ${MALLOC_LIBRARIES}
                      ${LIBEVENT_LIBRARIES}
                      ${COUCHBASE_NETWORK_LIBS}
                      ${MEMCACHED_EXTRA_LIBS})
add_sanitizers(memcached_daemon)
kv_enable_pch(memcached_daemon)

ADD_EXECUTABLE(memcached main.cc)
target_link_libraries(memcached PRIVATE
                      memcached_daemon
                      platform_cb_malloc_arena)
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
                       bucket_manager_test.cc
                       bucket_metering_stats_test.cc
                       bucket_stats_test.cc
                       bucket_validator_test.cc
                       buckets_test.cc
                       client_cert_config_test.cc
                       connection_unit_tests.cc
                       datatype_filter_test.cc
                       error_map_manager_test.cc
                       executor_tracing_test.cc
                       function_chain_test.cc
                       json_validator_test.cc
                       mc_time_test.cc
                       memcached_unit_test_main.cc
                       network_interface_description_test.cc
                       network_interface_test.cc
                       sdk_connection_manager_test.cc
                       settings_test.cc
                       sloppy_gauge_test.cc
                       ssl_utils_test.cc
                       tasks_test.cc
                       timings_test.cc
                       tls_configuration_test.cc)
add_dependencies(memcached_unit_tests Generate_kv_engine_X509_Test_Certificates)
cb_enable_unity_build(memcached_unit_tests)
add_sanitizers(memcached_unit_tests)
target_link_libraries(memcached_unit_tests PRIVATE
                      memcached_daemon
                      platform_cb_malloc_arena
                      GTest::gtest
                      GTest::gmock
                      statistics)
kv_enable_pch(memcached_unit_tests)
gtest_discover_tests(memcached_unit_tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        TEST_PREFIX memcached_unit_tests.
        ONE_CTEST_PER_SUITE
        ENVIRONMENT ${GTEST_ALL_SANITIZERS_ENV})
