include(CouchbaseGoogleTest)

INCLUDE_DIRECTORIES(AFTER ${Platform_SOURCE_DIR}/external)

ADD_EXECUTABLE(generate_test_cbsaslpw generate_test_cbsaslpw.cc
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h)
TARGET_LINK_LIBRARIES(generate_test_cbsaslpw cbsasl cJSON platform
                      ${OPENSSL_LIBRARIES})


set(memcached_testapp_SOURCES
    openssl_impl.cc
    ssl_impl.h
    subdoc_encoder.cc
    subdoc_encoder.h
    testapp.cc
    testapp.h
    testapp_arithmetic.cc
    testapp_arithmetic.h
    testapp_assert_helper.h
    testapp_audit.cc
    testapp_binprot.cc
    testapp_binprot.h
    testapp_bucket.cc
    testapp_bucket.h
    testapp_cert_tests.cc
    testapp_client_test.cc
    testapp_cmd_timers.cc
    testapp_client_test.h
    testapp_cluster_config.cc
    testapp_dcp.cc
    testapp_environment.cc
    testapp_environment.h
    testapp_errmap.cc
    testapp_flush.cc
    testapp_getset.cc
    testapp_hello.cc
    testapp_legacy_users.cc
    testapp_lock.cc
    testapp_misc.cc
    testapp_no_autoselect_default_bucket.cc
    testapp_persistence.cc
    testapp_rbac.cc
    testapp_regression.cc
    testapp_remove.cc
    testapp_sasl.cc
    testapp_sasl.h
    testapp_shutdown.cc
    testapp_ssl_utils.cc
    testapp_stats.cc
    testapp_stats.h
    testapp_subdoc.cc
    testapp_subdoc_common.cc
    testapp_subdoc_common.h
    testapp_subdoc_multipath.cc
    testapp_subdoc_perf.cc
    testapp_tests.cc
    testapp_timeout.cc
    testapp_touch.cc
    testapp_tracing.cc
    testapp_tune_mcbp_sla.cc
    testapp_withmeta.cc
    testapp_xattr.cc
    testapp_xattr.h
    utilities.cc
    utilities.h
    ${Memcached_SOURCE_DIR}/daemon/ssl_utils.cc
    ${Memcached_SOURCE_DIR}/daemon/ssl_utils.h
    )

# saslauthd is a Unix-only thingie
if (NOT WIN32)
  list(APPEND memcached_testapp_SOURCES
      saslauthd_mock.cc
      saslauthd_mock.h
      testapp_saslauthd.cc)
endif ()

add_executable(memcached_testapp ${memcached_testapp_SOURCES})
target_compile_options(memcached_testapp PRIVATE ${CB_CXX_FLAGS_NO_OPTIMIZE})
#target_include_directories(memcached_testapp PUBLIC ${spdlog_SOURCE_DIR}/include)
target_link_libraries(memcached_testapp
    memcached_daemon
    memcached_logger
    mcd_util
    mcutils
    mc_client_connection
    cbsasl
    cJSON
    platform
    dirutils
    gtest
    gtest_main
    ${SNAPPY_LIBRARIES}
    ${LIBEVENT_LIBRARIES}
    ${COUCHBASE_NETWORK_LIBS})
add_dependencies(memcached_testapp
    default_engine
    ep
    ewouldblock_engine
    memcached
    nobucket)
add_sanitizers(memcached_testapp)

# All tests should have a test timeout to avoid blocking our CI infrastructure
# if there is a bug in the code causing it to hang forever. The test timeout
# should be added right after the test is added to make it easy to see that
# it is present.
#
# Roughly calibrated to be 2x what the expected time is on the
# slowest Jenkins platform/config.

gtest_discover_tests(memcached_testapp
    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    TEST_PREFIX memcached_testapp.default.
    ONE_CTEST_PER_SUITE
    PROPERTIES TIMEOUT 300)

gtest_discover_tests(memcached_testapp
    EXTRA_ARGS -E ep
    WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
    TEST_PREFIX memcached_testapp.ep.
    ONE_CTEST_PER_SUITE
    PROPERTIES TIMEOUT 300)
