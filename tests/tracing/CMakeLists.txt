ADD_EXECUTABLE(tracing_test
               tracing_test.cc)

TARGET_LINK_LIBRARIES(tracing_test
  mcd_tracing memcached_daemon gtest gtest_main)

ADD_SANITIZERS(tracing_test)

ADD_TEST(NAME tracing-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND tracing_test)

set(mock_server_SOURCE
  ${Memcached_SOURCE_DIR}/programs/engine_testapp/mock_server.cc
  ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
  ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.cc
  $<TARGET_OBJECTS:memory_tracking>)

add_executable(tracing_bench
  tracing_bench.cc
  ${mock_server_SOURCE})

target_link_libraries(tracing_bench
  PRIVATE
    benchmark
    mcd_tracing
    mcd_util
    memcached_logger
    platform
    xattr
    ${MALLOC_LIBRARIES})
