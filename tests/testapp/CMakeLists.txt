IF (MEMCACHED_USE_GREENSTACK)
    LIST(APPEND MEMCACHED_GREENSTACK_SRC
         testapp_greenstack.cc
         testapp_greenstack.h)
ENDIF (MEMCACHED_USE_GREENSTACK)

list(APPEND TESTAPP_SOURCES
     ${Memcached_SOURCE_DIR}/daemon/ssl_utils.cc
     ${Memcached_SOURCE_DIR}/daemon/ssl_utils.h
     ${Memcached_SOURCE_DIR}/utilities/subdoc_encoder.cc
     ${MEMCACHED_GREENSTACK_SRC}
     testapp.cc
     testapp.h
     testapp_arithmetic.cc
     testapp_arithmetic.h
     testapp_audit.cc
     testapp_binprot.cc
     testapp_binprot.h
     testapp_bucket.cc
     testapp_bucket.h
     testapp_client_test.cc
     testapp_client_test.h
     testapp_environment.cc
     testapp_environment.h
     testapp_errmap.cc
     testapp_getset.cc
     testapp_lock.cc
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

ADD_DEPENDENCIES(memcached_testapp blackhole_logger default_engine
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
                      ${COUCHBASE_NETWORK_LIBS} ${OPENSSL_LIBRARIES})

target_compile_options(memcached_testapp PRIVATE ${CB_CXX_FLAGS_NO_OPTIMIZE})

# Run all unit tests we have't split into its own test target
ADD_TEST(NAME memcached-basic-unit-tests-bulk
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp
                    --gtest_filter=*-Transport/*:*PerfTest.*:ShutdownTest.*:RequireInitTest.*:*TransportProtocols*:*Greenstack*:AuditTest*:*ConnectionTimeout*:*ArithmeticTest*)

ADD_TEST(NAME memcached-basic-unit-tests-require-init
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=RequireInitTest.*)

# Run the Arithmetic tests
ADD_TEST(NAME memcached-arithmetic-unit-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=ArithmeticTest.*)

# Run the Audit tests
ADD_TEST(NAME memcached-audit-unit-tests
          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
          COMMAND memcached_testapp --gtest_filter=AuditTest.*)

# Run the Bucket tests
ADD_TEST(NAME memcached-bucket-unit-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=TransportProtocols/BucketTest.*)

# Run all of the lock tests
ADD_TEST(NAME memcached-lock-unit-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/LockTest.*)

# Run the Normal get set tests
ADD_TEST(NAME memcached-getset-unit-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=TransportProtocols/GetSetTest.*)

# Run the sasl tests
ADD_TEST(NAME memcached-sasl-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/SaslTest.*)

# Run the stats tests
ADD_TEST(NAME memcached-stats-unit-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=TransportProtocols/StatsTest.*)

# Run the unit tests for the memcached binary protocol over a plain socket
ADD_TEST(NAME memcached-mcbp-unit-tests-plain
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=Transport/*/Plain:-*/BucketTest*:-*Greenstack*)

# Run the unit tests for the memcached binary protocol over SSL
ADD_TEST(NAME memcached-mcbp-unit-tests-ssl
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=Transport/*/SSL:-*/BucketTest*:-*Greenstack*)

# Run the shutdown tests
ADD_TEST(NAME memcached-shutdown-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=ShutdownTest.*)

# Run the connection timeout tests
ADD_TEST(NAME memcached-connection-timeout-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=ConnectionTimeoutTest.*)

# Run the XATTR tests
ADD_TEST(NAME memcached-xattr-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=TransportProtocols/XattrTest.*)


IF (MEMCACHED_USE_GREENSTACK)
   ADD_TEST(NAME memcached-greenstack-tests
            WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
            COMMAND memcached_testapp --gtest_filter=*Greenstack*)
ENDIF(MEMCACHED_USE_GREENSTACK)

# For perf tests we also want GTest to output XML so we can plot the
# results in Jenkins.
ADD_TEST(NAME memcached-basic-perf-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=*PerfTest.* --gtest_output=xml:gtest_results/memcached_basic_perf_tests.xml)

# Test timeouts. Roughly calibrated to be 2x what the expected time is on the
# slowest Jenkins platform/config.
SET_TESTS_PROPERTIES(memcached-arithmetic-unit-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-audit-unit-tests PROPERTIES TIMEOUT 120)
SET_TESTS_PROPERTIES(memcached-basic-perf-tests PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-basic-unit-tests-bulk PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-basic-unit-tests-require-init PROPERTIES TIMEOUT 200)
SET_TESTS_PROPERTIES(memcached-bucket-unit-tests PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-getset-unit-tests PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-lock-unit-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-mcbp-unit-tests-plain PROPERTIES TIMEOUT 200)
SET_TESTS_PROPERTIES(memcached-mcbp-unit-tests-ssl PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-sasl-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-shutdown-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-stats-unit-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-connection-timeout-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-xattr-tests PROPERTIES TIMEOUT 60)
