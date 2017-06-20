INCLUDE_DIRECTORIES(AFTER ${Platform_SOURCE_DIR}/external)

list(APPEND ssl_impl_sources
     openssl_impl.cc)

list(APPEND TESTAPP_SOURCES
     ${Memcached_SOURCE_DIR}/daemon/ssl_utils.cc
     ${Memcached_SOURCE_DIR}/daemon/ssl_utils.h
     ${Memcached_SOURCE_DIR}/utilities/subdoc_encoder.cc
     ssl_impl.h
     ${ssl_impl_sources}
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
     testapp_client_test.h
     testapp_dcp.cc
     testapp_environment.cc
     testapp_environment.h
     testapp_errmap.cc
     testapp_flush.cc
     testapp_getset.cc
     testapp_legacy_users.cc
     testapp_lock.cc
     testapp_no_autoselect_default_bucket.cc
     testapp_rbac.cc
     testapp_remove.cc
     testapp_require_init.cc
     testapp_sasl.cc
     testapp_sasl.h
     testapp_shutdown.cc
     testapp_ssl_utils.cc
     testapp_stats.cc
     testapp_stats.h
     testapp_subdoc.cc
     testapp_subdoc_multipath.cc
     testapp_subdoc_perf.cc
     testapp_tests.cc
     testapp_timeout.cc
     testapp_touch.cc
     testapp_xattr.cc
     testapp_xattr.h
     utilities.cc
     utilities.h)

if (NOT WIN32)
    list(APPEND TESTAPP_SOURCES
         saslauthd_mock.cc
         saslauthd_mock.h
         testapp_saslauthd.cc)
endif (NOT WIN32)

ADD_EXECUTABLE(generate_test_cbsaslpw generate_test_cbsaslpw.cc
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h)
TARGET_LINK_LIBRARIES(generate_test_cbsaslpw cbsasl cJSON platform
                      ${OPENSSL_LIBRARIES})

ADD_EXECUTABLE(memcached_testapp ${TESTAPP_SOURCES})

ADD_DEPENDENCIES(memcached_testapp blackhole_logger default_engine ep
                 ewouldblock_engine memcached nobucket testapp_extension)

TARGET_LINK_LIBRARIES(memcached_testapp
                      memcached_daemon
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

target_compile_options(memcached_testapp PRIVATE ${CB_CXX_FLAGS_NO_OPTIMIZE})

# All tests should have a test timeout to avoid blocking our CI infrastructure
# if there is a bug in the code causing it to hang forever. The test timeout
# should be added right after the test is added to make it easy to see that
# it is present.
#
# Roughly calibrated to be 2x what the expected time is on the
# slowest Jenkins platform/config.

# Run all unit tests we have't split into its own test target
ADD_TEST(NAME memcached-basic-unit-tests-bulk
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp
                    --gtest_filter=*-Transport/*:*PerfTest.*:ShutdownTest.*:RequireInitTest.*:*TransportProtocols*:AuditTest*:*ConnectionTimeout*:*ArithmeticTest*:SslCertTest.*)
SET_TESTS_PROPERTIES(memcached-basic-unit-tests-bulk PROPERTIES TIMEOUT 60)

ADD_TEST(NAME memcached-basic-unit-tests-require-init
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=RequireInitTest.*)
SET_TESTS_PROPERTIES(memcached-basic-unit-tests-require-init PROPERTIES TIMEOUT 200)

# Run the Arithmetic tests
ADD_TEST(NAME memcached-arithmetic-unit-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=ArithmeticTest.*)
SET_TESTS_PROPERTIES(memcached-arithmetic-unit-tests PROPERTIES TIMEOUT 120)

# Run the Audit tests
ADD_TEST(NAME memcached-audit-unit-tests
          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
          COMMAND memcached_testapp --gtest_filter=AuditTest.*)
SET_TESTS_PROPERTIES(memcached-audit-unit-tests PROPERTIES TIMEOUT 120)

# Run the Bucket tests
ADD_TEST(NAME memcached-bucket-unit-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=TransportProtocols/BucketTest.*)
SET_TESTS_PROPERTIES(memcached-bucket-unit-tests PROPERTIES TIMEOUT 400)

# Run the DCP tests
ADD_TEST(NAME memcached-dcp-unit-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/DcpTest.*)
SET_TESTS_PROPERTIES(memcached-dcp-unit-tests PROPERTIES TIMEOUT 120)

# Run the Flush tests
ADD_TEST(NAME memcached-flush-unit-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/FlushTest.*)
SET_TESTS_PROPERTIES(memcached-flush-unit-tests PROPERTIES TIMEOUT 120)

# Run the tests with legacy users
ADD_TEST(NAME memcached-legacy-users-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=LegacyUsersTest.*)
SET_TESTS_PROPERTIES(memcached-legacy-users-tests PROPERTIES TIMEOUT 60)

# Run all of the lock tests
ADD_TEST(NAME memcached-lock-unit-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/LockTest.*)
SET_TESTS_PROPERTIES(memcached-lock-unit-tests PROPERTIES TIMEOUT 120)

# Run the Normal get set tests
ADD_TEST(NAME memcached-getset-unit-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=TransportProtocols/GetSetTest.*)
SET_TESTS_PROPERTIES(memcached-getset-unit-tests PROPERTIES TIMEOUT 400)

# Run the sasl tests
ADD_TEST(NAME memcached-sasl-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/SaslTest.*)
SET_TESTS_PROPERTIES(memcached-sasl-tests PROPERTIES TIMEOUT 120)

# Run the stats tests
ADD_TEST(NAME memcached-stats-unit-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=TransportProtocols/StatsTest.*)
SET_TESTS_PROPERTIES(memcached-stats-unit-tests PROPERTIES TIMEOUT 120)

# Run the unit tests for the memcached binary protocol over a plain socket
ADD_TEST(NAME memcached-mcbp-unit-tests-plain
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=Transport/*/Plain:-*/BucketTest*)
SET_TESTS_PROPERTIES(memcached-mcbp-unit-tests-plain PROPERTIES TIMEOUT 200)

# Run the unit tests for the memcached binary protocol over SSL
ADD_TEST(NAME memcached-mcbp-unit-tests-ssl
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=Transport/*/SSL:-*/BucketTest*)
SET_TESTS_PROPERTIES(memcached-mcbp-unit-tests-ssl PROPERTIES TIMEOUT 400)

# Run the RBAC tests
ADD_TEST(NAME memcached-rbac-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/RbacTest.*)
SET_TESTS_PROPERTIES(memcached-rbac-tests PROPERTIES TIMEOUT 120)

# Run the batch to verify remove
ADD_TEST(NAME memcached-remove-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/RemoveTest.*)
SET_TESTS_PROPERTIES(memcached-remove-tests PROPERTIES TIMEOUT 120)

# Run the shutdown tests
ADD_TEST(NAME memcached-shutdown-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=ShutdownTest.*)
SET_TESTS_PROPERTIES(memcached-shutdown-tests PROPERTIES TIMEOUT 120)

# Run the connection timeout tests
ADD_TEST(NAME memcached-connection-timeout-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=ConnectionTimeoutTest.*)
SET_TESTS_PROPERTIES(memcached-connection-timeout-tests PROPERTIES TIMEOUT 120)

# Run the touch related tests
ADD_TEST(NAME memcached-touch-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/TouchTest.*)
SET_TESTS_PROPERTIES(memcached-touch-tests PROPERTIES TIMEOUT 120)

# Run the XATTR tests
ADD_TEST(NAME memcached-xattr-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/XattrTest.*)
SET_TESTS_PROPERTIES(memcached-xattr-tests PROPERTIES TIMEOUT 120)

# Run the SSL cert tests
ADD_TEST(NAME memcached-ssl-cert-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=SslCertTest.*)
SET_TESTS_PROPERTIES(memcached-ssl-cert-tests PROPERTIES TIMEOUT 120)

# For perf tests we also want GTest to output XML so we can plot the
# results in Jenkins.
ADD_TEST(NAME memcached-basic-perf-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=*PerfTest.* --gtest_output=xml:gtest_results/memcached_basic_perf_tests.xml)
SET_TESTS_PROPERTIES(memcached-basic-perf-tests PROPERTIES TIMEOUT 400)

# Verify that we don't connect to a password protected default bucket
ADD_TEST(NAME memcached-no-autoselect-default-bucket-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/NoAutoselectDefaultBucketTest.*)
SET_TESTS_PROPERTIES(memcached-no-autoselect-default-bucket-tests PROPERTIES TIMEOUT 120)
