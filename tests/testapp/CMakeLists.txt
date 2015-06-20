ADD_EXECUTABLE(memcached_testapp
               bucket_tests.cc
               testapp.cc testapp.h
               testapp_subdoc.cc
               ${Memcached_SOURCE_DIR}/programs/utilities.c)
TARGET_LINK_LIBRARIES(memcached_testapp mcd_util cbsasl cJSON platform gtest gtest_main ${SNAPPY_LIBRARIES} ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS} ${OPENSSL_LIBRARIES})

# Note: As well as the Transport (plain/SSL) tests there are also some
# other unit tests in memcached_testapp. Therefore run all _but_ the
# SSL tests in 'plain'.
ADD_TEST(NAME memcached-basic-unit-tests-plain
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=-PlainOrSSL/*/1)
ADD_TEST(NAME memcached-basic-unit-tests-SSL
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_testapp --gtest_filter=PlainOrSSL/*/1)
