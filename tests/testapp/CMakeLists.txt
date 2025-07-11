include(CouchbaseGoogleTest)

add_library(testapp_auth_provider OBJECT auth_provider.h auth_provider.cc)
kv_enable_pch(testapp_auth_provider)
cb_add_test_executable(testapp_auth_provider_test auth_provider_test.cc)
kv_enable_pch(testapp_auth_provider_test)
kv_enable_pch(testapp_auth_provider_test)
target_link_libraries(testapp_auth_provider PUBLIC platform)
target_link_libraries(testapp_auth_provider_test
        PRIVATE
                testapp_auth_provider
                cbsasl
                GTest::gtest
                GTest::gtest_main
                mcbp)

add_test(NAME testapp_auth_provider_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND testapp_auth_provider_test)

cb_add_test_executable(memcached_testapp
    subdoc_encoder.cc
    subdoc_encoder.h
    testapp.cc
    testapp.h
    testapp_access_scanner.cc
    testapp_arithmetic.cc
    testapp_audit.cc
    testapp_audit.h
    testapp_binprot.cc
    testapp_binprot.h
    testapp_bucket.cc
    testapp_cert_tests.cc
    testapp_client_test.cc
    testapp_client_test.h
    testapp_cluster_config.cc
    testapp_cmd_timers.cc
    testapp_collections.cc
    testapp_compression.cc
    testapp_dcp.cc
    testapp_dcp_consumer.cc
    testapp_deprecated_commands.cc
    testapp_durability.cc
    testapp_encryption.cc
    testapp_environment.cc
    testapp_environment.h
    testapp_errmap.cc
    testapp_external_auth.cc
    testapp_fusion.cc
    testapp_getset.cc
    testapp_get_random_key.cc
    testapp_hello.cc
    testapp_interfaces.cc
    testapp_ioctl.cc
    testapp_ipv6.cc
    testapp_lock.cc
    testapp_logging.cc
    testapp_maxconn.cc
    testapp_misc.cc
    testapp_not_supported.cc
    testapp_pause_resume.cc
    testapp_persistence.cc
    testapp_range_scan.cc
    testapp_rbac.cc
    testapp_regression.cc
    testapp_remove.cc
    testapp_sasl.cc
    testapp_shutdown.cc
    testapp_stats.cc
    testapp_subdoc.cc
    testapp_subdoc_common.cc
    testapp_subdoc_common.h
    testapp_subdoc_concurrency.cc
    testapp_subdoc_multipath.cc
    testapp_subdoc_perf.cc
    testapp_tls.cc
    testapp_touch.cc
    testapp_tracing.cc
    testapp_ttl.cc
    testapp_tune_mcbp_sla.cc
    testapp_withmeta.cc
    testapp_xattr.cc
    testapp_xattr_deleted.cc
    testapp_xattr.h
    ${Memcached_SOURCE_DIR}/logger/logger_test_fixture.cc)

target_include_directories(memcached_testapp PRIVATE ${Memcached_BINARY_DIR} ${Platform_SOURCE_DIR}/external)

target_link_libraries(memcached_testapp PRIVATE
    testapp_auth_provider
    memcached_daemon
    memcached_logger
    mcd_util
    mcd_test_util
    mc_client_connection
    cbsasl
    json_validator
    platform
    platform_cb_malloc_arena
    GTest::gmock
    GTest::gtest
    GTest::gtest_main
    ${SNAPPY_LIBRARIES}
    ${LIBEVENT_LIBRARIES}
    ${COUCHBASE_NETWORK_LIBS})
add_dependencies(memcached_testapp
    memcached
    generate_audit_descriptors
    Generate_kv_engine_X509_Test_Certificates
    check_pragma_once)
add_sanitizers(memcached_testapp)
cb_enable_unity_build(memcached_testapp)
kv_enable_pch(memcached_testapp)

# All tests should have a test timeout to avoid blocking our CI infrastructure
# if there is a bug in the code causing it to hang forever. The test timeout
# should be added right after the test is added to make it easy to see that
# it is present.
#
# Roughly calibrated to be 2x what the expected time is on the
# slowest Jenkins platform/config.

OPTION(CB_VERBOSE_MEMCACHED_TESTAPP
        "Trigger verbose output of memcached_testapp"
        OFF)

set(VERBOSE_EXTRA_ARG "")
if(CB_VERBOSE_MEMCACHED_TESTAPP)
    message(STATUS "Setting memcached_testapp to verbose output")
    set(VERBOSE_EXTRA_ARG "-v")
endif(CB_VERBOSE_MEMCACHED_TESTAPP)

# MB-32484: To attempt to identify the cause of the intermittent timeouts in
# memcached_testapp, use the `timeout` program to signal memcached_testapp with
# SIGABRT when we timeout; to allow a core dump to be generated of where the
# process is stuck.
# Using a timeout value (240s) before the CTest timeout of 300s below so
# `timeout` fires first.
find_program (TIMEOUT_EXE timeout)
if (TIMEOUT_EXE AND NOT WIN32)
    set_property(TARGET memcached_testapp
             PROPERTY CB_TEST_EXECUTOR ${TIMEOUT_EXE} --preserve-status --signal=ABRT 240s)
endif()

gtest_discover_tests(memcached_testapp
    EXTRA_ARGS ${VERBOSE_EXTRA_ARG}
    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    TEST_PREFIX memcached_testapp.
    ONE_CTEST_PER_SUITE
    DISCOVERY_TIMEOUT 60
    PROPERTIES TIMEOUT 300
               ENVIRONMENT ${GTEST_ALL_SANITIZERS_ENV})

gtest_discover_tests(memcached_testapp
        EXTRA_ARGS -c item_eviction_policy=full_eviction ${VERBOSE_EXTRA_ARG}
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        TEST_PREFIX memcached_testapp.full_eviction.
        ONE_CTEST_PER_SUITE
        DISCOVERY_TIMEOUT 60
        PROPERTIES TIMEOUT 300
        ENVIRONMENT ${THREAD_SANITIZER_TEST_ENV})

if (TARGET magma)
    gtest_discover_tests(memcached_testapp
            EXTRA_ARGS -c backend=magma -c max_size=209715200 ${VERBOSE_EXTRA_ARG}
            WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
            TEST_PREFIX memcached_testapp.magma.
            ONE_CTEST_PER_SUITE
            DISCOVERY_TIMEOUT 60
            PROPERTIES TIMEOUT 300
            ENVIRONMENT ${THREAD_SANITIZER_TEST_ENV})
endif (TARGET magma)
