include(CouchbaseGoogleTest)

INCLUDE_DIRECTORIES(AFTER ${Platform_SOURCE_DIR}/external)

ADD_EXECUTABLE(generate_test_cbsaslpw generate_test_cbsaslpw.cc)
TARGET_LINK_LIBRARIES(generate_test_cbsaslpw cbsasl platform
                      ${OPENSSL_LIBRARIES})
add_sanitizers(generate_test_cbsaslpw)

add_library(testapp_auth_provider OBJECT auth_provider.h auth_provider.cc)
add_executable(testapp_auth_provider_test auth_provider_test.cc
               $<TARGET_OBJECTS:testapp_auth_provider>)
target_link_libraries(testapp_auth_provider_test mcbp cbsasl platform gtest gtest_main)
add_test(NAME testapp_auth_provider_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND testapp_auth_provider_test)

set(memcached_testapp_SOURCES
    $<TARGET_OBJECTS:testapp_auth_provider>
    openssl_impl.cc
    ssl_impl.h
    subdoc_encoder.cc
    subdoc_encoder.h
    testapp.cc
    testapp.h
    testapp_arithmetic.cc
    testapp_assert_helper.h
    testapp_audit.cc
    testapp_binprot.cc
    testapp_binprot.h
    testapp_bucket.cc
    testapp_cert_tests.cc
    testapp_client_test.cc
    testapp_client_test.h
    testapp_cluster_config.cc
    testapp_cmd_timers.cc
    testapp_collections.cc
    testapp_dcp.cc
    testapp_durability.cc
    testapp_environment.cc
    testapp_environment.h
    testapp_errmap.cc
    testapp_external_auth.cc
    testapp_flush.cc
    testapp_getset.cc
    testapp_hello.cc
    testapp_interfaces.cc
    testapp_ipv6.cc
    testapp_legacy_users.cc
    testapp_lock.cc
    testapp_logging.cc
    testapp_maxconn.cc
    testapp_misc.cc
    testapp_no_autoselect_default_bucket.cc
    testapp_not_supported.cc
    testapp_persistence.cc
    testapp_rbac.cc
    testapp_regression.cc
    testapp_remove.cc
    testapp_sasl.cc
    testapp_shutdown.cc
    testapp_ssl_utils.cc
    testapp_stats.cc
    testapp_subdoc.cc
    testapp_subdoc_common.cc
    testapp_subdoc_common.h
    testapp_subdoc_multipath.cc
    testapp_subdoc_perf.cc
    testapp_tests.cc
    testapp_tls.cc
    testapp_topkeys.cc
    testapp_touch.cc
    testapp_tracing.cc
    testapp_ttl.cc
    testapp_tune_mcbp_sla.cc
    testapp_withmeta.cc
    testapp_xattr.cc
    testapp_xattr_deleted.cc
    testapp_xattr.h
    ${Memcached_SOURCE_DIR}/logger/logger_test_fixture.cc)

add_executable(memcached_testapp ${memcached_testapp_SOURCES})
target_compile_options(memcached_testapp PRIVATE ${CB_CXX_FLAGS_NO_OPTIMIZE})
target_include_directories(memcached_testapp PRIVATE ${Memcached_BINARY_DIR})

target_link_libraries(memcached_testapp
    memcached_daemon
    memcached_logger
    mcd_util
    mc_client_connection
    cbsasl
    platform
    platform_cb_malloc_arena
    dirutils
    gtest
    gtest_main
    ${FOLLY_LIBRARIES}
    ${SNAPPY_LIBRARIES}
    ${LIBEVENT_LIBRARIES}
    ${COUCHBASE_NETWORK_LIBS})
add_dependencies(memcached_testapp memcached)
add_sanitizers(memcached_testapp)

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
    TEST_PREFIX memcached_testapp.default.
    ONE_CTEST_PER_SUITE
    DISCOVERY_TIMEOUT 60
    PROPERTIES TIMEOUT 300)

gtest_discover_tests(memcached_testapp
    EXTRA_ARGS -E ep ${VERBOSE_EXTRA_ARG}
    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    TEST_PREFIX memcached_testapp.ep.
    ONE_CTEST_PER_SUITE
    DISCOVERY_TIMEOUT 60
    PROPERTIES TIMEOUT 300)
