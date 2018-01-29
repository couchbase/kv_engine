INCLUDE_DIRECTORIES(AFTER ${Platform_SOURCE_DIR}/external)

ADD_EXECUTABLE(generate_test_cbsaslpw generate_test_cbsaslpw.cc
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h)
TARGET_LINK_LIBRARIES(generate_test_cbsaslpw cbsasl cJSON platform
                      ${OPENSSL_LIBRARIES})


add_library(memcached_testapp_core OBJECT
            ${Memcached_SOURCE_DIR}/utilities/subdoc_encoder.cc
            openssl_impl.cc
            ssl_impl.h
            testapp.cc
            testapp.h
            testapp_assert_helper.h
            testapp_binprot.cc
            testapp_binprot.h
            testapp_client_test.cc
            testapp_client_test.h
            testapp_environment.cc
            testapp_environment.h
            utilities.cc
            utilities.h)
target_compile_options(memcached_testapp_core PRIVATE ${CB_CXX_FLAGS_NO_OPTIMIZE})
target_include_directories(memcached_testapp_core PUBLIC ${spdlog_SOURCE_DIR}/include)

# Create a function to generate the various test cases
function(add_unit_test_suite)
    set(arg_TIMEOUT 120)
    cmake_parse_arguments("arg" "" "ENGINE;NAME;TIMEOUT;OUTPUT" "SOURCE" ${ARGN})
    if (NOT arg_NAME)
        MESSAGE(FATAL_ERROR "add_unit_test_suite: NAME must be specified")
    endif()
    if (NOT arg_TIMEOUT)
        MESSAGE(FATAL_ERROR "add_unit_test_suite: TIMEOUT must be specified")
    endif()

    if (NOT arg_SOURCE)
        MESSAGE(FATAL_ERROR "add_unit_test_suite: SOURCE must be specified")
    endif()

    add_executable(memcached-${arg_NAME}-tests ${arg_SOURCE}
                   $<TARGET_OBJECTS:memcached_testapp_core>)
    target_compile_options(memcached-${arg_NAME}-tests PRIVATE ${CB_CXX_FLAGS_NO_OPTIMIZE})
    target_link_libraries(memcached-${arg_NAME}-tests
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
    add_dependencies(memcached-${arg_NAME}-tests
                     default_engine ep
                     ewouldblock_engine
                     memcached nobucket)

    if (arg_ENGINE)
        add_test(NAME memcached-${arg_NAME}-${arg_ENGINE}-tests
                 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
                 COMMAND memcached-${arg_NAME}-tests ${arg_OUTPUT} -E ${arg_ENGINE})
        set_tests_properties(memcached-${arg_NAME}-${arg_ENGINE}-tests PROPERTIES TIMEOUT ${arg_TIMEOUT})
    endif()
    add_test(NAME memcached-${arg_NAME}-tests
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached-${arg_NAME}-tests ${arg_OUTPUT})
    set_tests_properties(memcached-${arg_NAME}-tests PROPERTIES TIMEOUT ${arg_TIMEOUT})
endfunction()

add_unit_test_suite(NAME ssl-utilities
                    TIMEOUT 30
                    SOURCE
                    ${Memcached_SOURCE_DIR}/daemon/ssl_utils.cc
                    ${Memcached_SOURCE_DIR}/daemon/ssl_utils.h
                    testapp_ssl_utils.cc)

# All tests should have a test timeout to avoid blocking our CI infrastructure
# if there is a bug in the code causing it to hang forever. The test timeout
# should be added right after the test is added to make it easy to see that
# it is present.
#
# Roughly calibrated to be 2x what the expected time is on the
# slowest Jenkins platform/config.

# Run the Arithmetic tests
add_unit_test_suite(NAME arithmetic
                    ENGINE ep
                    TIMEOUT 240
                    SOURCE testapp_arithmetic.cc testapp_arithmetic.h)

# Run the Audit tests
add_unit_test_suite(NAME audit
                    TIMEOUT 120
                    SOURCE testapp_audit.cc)

# Run the Bucket tests
add_unit_test_suite(NAME bucket
                    TIMEOUT 400
                    SOURCE testapp_bucket.cc testapp_bucket.h)

# Run the DCP tests
add_unit_test_suite(NAME dcp
                    TIMEOUT 120
                    SOURCE testapp_dcp.cc)

add_unit_test_suite(NAME errmap
                    TIMEOUT 120
                    SOURCE testapp_errmap.cc)

# Run the Flush tests
add_unit_test_suite(NAME flush
                    ENGINE ep
                    TIMEOUT 120
                    SOURCE testapp_flush.cc)

# Run the tests with legacy users
add_unit_test_suite(NAME legacy-users
                    TIMEOUT 60
                    SOURCE testapp_legacy_users.cc)

# Run all of the lock tests
add_unit_test_suite(NAME lock
                    ENGINE ep
                    TIMEOUT 120
                    SOURCE testapp_lock.cc)

# Run the Normal get set tests
add_unit_test_suite(NAME get-set
                    ENGINE ep
                    TIMEOUT 400
                    SOURCE testapp_getset.cc)

# Run the sasl tests
if (WIN32)
    # saslauthd is a Unix-only thingie
    add_unit_test_suite(NAME sasl
                        TIMEOUT 120
                        SOURCE testapp_sasl.cc testapp_sasl.h)
else ()
    add_unit_test_suite(NAME sasl
                        TIMEOUT 120
                        SOURCE
                        testapp_sasl.cc testapp_sasl.h
                        saslauthd_mock.cc
                        saslauthd_mock.h
                        testapp_saslauthd.cc)
endif ()

# Run the stats tests
add_unit_test_suite(NAME stats
                    TIMEOUT 120
                    SOURCE testapp_stats.cc testapp_stats.h)

# Run the unit tests for the memcached binary protocol over a plain socket
add_unit_test_suite(NAME legacy-plain
                    TIMEOUT 200
                    SOURCE testapp_legacy_plain.cc testapp_tests.cc)

# Run the unit tests for the memcached binary protocol over SSL
add_unit_test_suite(NAME legacy-ssl
                    TIMEOUT 200
                    SOURCE testapp_legacy_ssl.cc testapp_tests.cc)

# Run the RBAC tests
add_unit_test_suite(NAME persistence
                    ENGINE ep
                    TIMEOUT 60
                    SOURCE testapp_persistence.cc)

# Run the RBAC tests
add_unit_test_suite(NAME rbac
                    ENGINE ep
                    TIMEOUT 120
                    SOURCE testapp_rbac.cc)

# Run the regression tests
add_unit_test_suite(NAME regression
                    ENGINE ep
                    TIMEOUT 60
                    SOURCE testapp_regression.cc)

# Run the batch to verify remove
add_unit_test_suite(NAME remove
                    ENGINE ep
                    TIMEOUT 120
                    SOURCE testapp_remove.cc)

# Run the shutdown tests
add_unit_test_suite(NAME shutdown
                    TIMEOUT 120
                    SOURCE testapp_shutdown.cc)

# Run the subdoc functional tests
add_unit_test_suite(NAME subdoc
                    TIMEOUT 120
                    SOURCE testapp_subdoc.cc testapp_subdoc_common.cc)

# Run the connection timeout tests
add_unit_test_suite(NAME connection-timeout
                    TIMEOUT 120
                    SOURCE testapp_timeout.cc)

# Run the touch related tests
add_unit_test_suite(NAME touch
                    ENGINE ep
                    TIMEOUT 120
                    SOURCE testapp_touch.cc)

# Run the tests to tune the MCBP SLA via an IOCTL
add_unit_test_suite(NAME tune-mcbp-lsa
                    TIMEOUT 100
                    SOURCE testapp_tune_mcbp_sla.cc)

# Run the XATTR tests
add_unit_test_suite(NAME xattr
                    ENGINE ep
                    TIMEOUT 240
                    SOURCE testapp_xattr.h testapp_xattr.cc)

# Run the SSL cert tests
add_unit_test_suite(NAME ssl-cert
                    TIMEOUT 120
                    SOURCE testapp_cert_tests.cc)

add_unit_test_suite(NAME with-meta
                    ENGINE ep
                    TIMEOUT 120
                    SOURCE testapp_withmeta.cc)

# Verify that we don't connect to a password protected default bucket
add_unit_test_suite(NAME no-autoselect-default-bucket
                    TIMEOUT 120
                    SOURCE testapp_no_autoselect_default_bucket.cc)

# For perf tests we also want GTest to output XML so we can plot the
# results in Jenkins.
add_unit_test_suite(NAME subdoc-perf
                    TIMEOUT 400
                    SOURCE testapp_subdoc_perf.cc testapp_subdoc_common.cc
                    OUTPUT --gtest_output=xml:gtest_results/memcached_basic_perf_tests.xml)

add_unit_test_suite(NAME subdoc-multipath
                    TIMEOUT 400
                    SOURCE testapp_subdoc_multipath.cc testapp_subdoc_common.cc)

# Test that we correctly set and get cluster configuration (and that
# the cluster configuration is piggybacked on a not-my-vbucket response
# (and properly deduplicated)
add_unit_test_suite(NAME cluster-config
                    TIMEOUT 400
                    SOURCE testapp_cluster_config.cc)

# Unit test verifying different parts of hello
add_unit_test_suite(NAME hello TIMEOUT 120 SOURCE testapp_hello.cc)
