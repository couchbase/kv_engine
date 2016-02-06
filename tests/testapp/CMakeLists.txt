ADD_EXECUTABLE(memcached_testapp
               ${Memcached_SOURCE_DIR}/programs/utilities.cc
               ${Memcached_SOURCE_DIR}/utilities/subdoc_encoder.cc
               testapp.cc
               testapp.h
               testapp_audit.cc
               testapp_binprot.cc
               testapp_binprot.h
               testapp_bucket.cc
               testapp_bucket.h
               testapp_client_test.cc
               testapp_client_test.h
               testapp_connection.cc
               testapp_connection.h
               testapp_getset.cc
               testapp_getset.h
               testapp_greenstack.cc
               testapp_greenstack.h
               testapp_greenstack_connection.cc
               testapp_greenstack_connection.h
               testapp_mcbp_connection.cc
               testapp_mcbp_connection.h
               testapp_rbac.cc
               testapp_rbac.h
               testapp_require_init.cc
               testapp_sasl.cc
               testapp_sasl.h
               testapp_shutdown.cc
               testapp_stats.cc
               testapp_stats.h
               testapp_subdoc.cc
               testapp_subdoc_multipath.cc
               testapp_subdoc_perf.cc)

ADD_DEPENDENCIES(memcached_testapp blackhole_logger default_engine
                 ewouldblock_engine memcached nobucket testapp_extension)

TARGET_LINK_LIBRARIES(memcached_testapp
                      mcd_util
                      cbsasl
                      greenstack
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
                    --gtest_filter=*-PlainOrSSL/*:*PerfTest.*:ShutdownTest.*:RequireInitTest.*:*TransportProtocols*:*Greenstack*:AuditTest*)

ADD_TEST(NAME memcached-basic-unit-tests-require-init
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=RequireInitTest.*)

# Run the Audit tests
ADD_TEST(NAME memcached-audit-unit-tests
          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
          COMMAND memcached_testapp --gtest_filter=AuditTest.*)

# Run the Bucket tests
ADD_TEST(NAME memcached-bucket-unit-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=TransportProtocols/BucketTest.*)

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
        COMMAND memcached_testapp --gtest_filter=PlainOrSSL/*/0:-*/BucketTest*:-*Greenstack*)

# Run the unit tests for the memcached binary protocol over SSL
ADD_TEST(NAME memcached-mcbp-unit-tests-ssl
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=PlainOrSSL/*/1:-*/BucketTest*:-*Greenstack*)

# Run the shutdown tests
ADD_TEST(NAME memcached-shutdown-tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND memcached_testapp --gtest_filter=ShutdownTest.*)

#Disabled while making greenstack use bufferevents
#ADD_TEST(NAME memcached-greenstack-tests
#        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
#        COMMAND memcached_testapp --gtest_filter=*Greenstack*)

# For perf tests we also want GTest to output XML so we can plot the
# results in Jenkins.
ADD_TEST(NAME memcached-basic-perf-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=*PerfTest.* --gtest_output=xml:gtest_results/memcached_basic_perf_tests.xml)

# Test timeouts. Roughly calibrated to be 2x what the expected time is on the
# slowest Jenkins platform/config.
SET_TESTS_PROPERTIES(memcached-audit-unit-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-basic-perf-tests PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-basic-unit-tests-bulk PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-basic-unit-tests-require-init PROPERTIES TIMEOUT 200)
SET_TESTS_PROPERTIES(memcached-bucket-unit-tests PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-getset-unit-tests PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-mcbp-unit-tests-plain PROPERTIES TIMEOUT 200)
SET_TESTS_PROPERTIES(memcached-mcbp-unit-tests-ssl PROPERTIES TIMEOUT 400)
SET_TESTS_PROPERTIES(memcached-sasl-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-shutdown-tests PROPERTIES TIMEOUT 60)
SET_TESTS_PROPERTIES(memcached-stats-unit-tests PROPERTIES TIMEOUT 60)
