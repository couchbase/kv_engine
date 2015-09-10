ADD_EXECUTABLE(memcached_testapp
               ${Memcached_SOURCE_DIR}/programs/utilities.c
               ${Memcached_SOURCE_DIR}/utilities/subdoc_encoder.cc
               bucket_tests.cc
               testapp.cc testapp.h
               testapp_binprot.cc
               testapp_binprot.h
               testapp_rbac.cc
               testapp_rbac.h
               testapp_shutdown.cc
               testapp_subdoc.cc
               testapp_subdoc_multipath.cc
               testapp_subdoc_perf.cc
	       )
TARGET_LINK_LIBRARIES(memcached_testapp mcd_util cbsasl cJSON platform dirutils gtest gtest_main ${SNAPPY_LIBRARIES} ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS} ${OPENSSL_LIBRARIES})

# Note: As well as the Transport (plain/SSL) tests there are also some
# other unit tests in memcached_testapp. Therefore run all _but_ the
# SSL tests in 'plain'.
# In both cases we skip the PerfTests (run seperately below).
ADD_TEST(NAME memcached-basic-unit-tests-plain
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=*-PlainOrSSL/*/1:*PerfTest.*)
ADD_TEST(NAME memcached-basic-unit-tests-SSL
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=PlainOrSSL/*/1-*PerfTest.*)

# For perf tests we also want GTest to output XML so we can plot the
# results in Jenkins.
ADD_TEST(NAME memcached-basic-perf-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=*PerfTest.* --gtest_output=xml:gtest_results/memcached_basic_perf_tests.xml)
