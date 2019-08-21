INCLUDE_DIRECTORIES(BEFORE SYSTEM ${hdr_histogram_SOURCE_DIR}/src)

add_library(mock_server STATIC
            mock_server.cc
            mock_server.h
            ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.cc
            ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.h
            ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
            ${Memcached_SOURCE_DIR}/daemon/enginemap.cc
            ${Memcached_SOURCE_DIR}/daemon/enginemap.h
            ${Memcached_SOURCE_DIR}/daemon/bucket_type.cc
            ${Memcached_SOURCE_DIR}/daemon/bucket_type.h
            $<TARGET_OBJECTS:memory_tracking>)
set_property(TARGET mock_server PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mock_server INTERFACE
                      memcached_logger
                      mcd_tracing
                      hdr_histogram_static
                      mcd_util
                      platform
                      xattr
                      ${MALLOC_LIBRARIES})

add_executable(engine_testapp engine_testapp.cc)
target_link_libraries(engine_testapp PRIVATE
        phosphor
        platform
        mock_server
                      crash_engine
                      default_engine
                      ep
                      ewouldblock_engine
                      nobucket
        ${COUCHBASE_NETWORK_LIBS})

add_sanitizers(engine_testapp)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_dependencies(engine_testapp
                     ep
                     default_engine
                     basic_engine_testsuite
                     ep_testsuite_basic
                     ep_testsuite_checkpoint
                     ep_testsuite_dcp
                     ep_testsuite
                     ep_testsuite_xdcr
                     timing_tests)
endif ()

install(TARGETS engine_testapp
        RUNTIME DESTINATION bin)

add_test(NAME memcached-basic-engine-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND engine_testapp -E mc
                            -T basic_engine_testsuite.so)
