add_library(mcbp STATIC
            ${Memcached_SOURCE_DIR}/include/mcbp/codec/frameinfo.h
            ${Memcached_SOURCE_DIR}/include/mcbp/mcbp.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/datatype.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/feature.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/framebuilder.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/magic.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/opcode.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/request.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/response.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/status.h
            crc_sink.cc
            datatype.cc
            dcp_snapshot_marker_codec.cc
            dcp_stream_end_status.cc
            dump.cc
            ewb_encode.cc
            feature.cc
            formatters.cc
            frameinfo.cc
            gdb_dump_parser.cc
            json_utilities.cc
            lldb_dump_parser.cc
            magic.cc
            opcode.cc
            request.cc
            response.cc
            header.cc
            range_scan_continue_codec.cc
            sla.cc
            status.cc
            )

target_link_libraries(mcbp
        PUBLIC
                platform
        PRIVATE
                Folly::headers
                fmt::fmt
                mcd_util)
set_property(TARGET mcbp PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(mcbp)
cb_enable_unity_build(mcbp)

cb_add_test_executable(mcbp_packet_printer mcbp_packet_printer.cc)
kv_enable_pch(mcbp_packet_printer)
target_link_libraries(mcbp_packet_printer PRIVATE mcbp platform)
add_sanitizers(mcbp_packet_printer)

cb_add_test_executable(mcbp_info mcbp_info.cc)
kv_enable_pch(mcbp_info)
target_link_libraries(mcbp_info PRIVATE Folly::headers mcbp platform)
add_sanitizers(mcbp_info)

cb_add_test_executable(mcbp_unit_tests
               feature_test.cc
               formatters_test.cc
               framebuilder_test.cc
               json_utilities_test.cc
               magic_test.cc
               mcbp_dump_parser_test.cc
               request_test.cc
               sla_test.cc)
kv_enable_pch(mcbp_unit_tests)
target_link_libraries(mcbp_unit_tests
        PRIVATE
                Folly::headers
                GTest::gtest
                GTest::gtest_main
                mcbp)
add_test(NAME mcbp_unit_tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND mcbp_unit_tests)
add_sanitizers(mcbp_unit_tests)
cb_enable_unity_build(mcbp_unit_tests)

# Fuzz tests are linked into the mcbp_fuzz_test executable.
if(HAVE_FUZZTEST)
    cb_add_test_executable(mcbp_fuzz_test mcbp_fuzz_test.cc)
    target_include_directories(mcbp_fuzz_test SYSTEM PRIVATE ${Memcached_BINARY_DIR})
    cb_link_fuzztest(mcbp_fuzz_test)
    cb_link_fuzztest_main(mcbp_fuzz_test)
    target_link_libraries(mcbp_fuzz_test PRIVATE memcached_daemon)
endif()

if(FUZZTEST_FUZZING_MODE)
    # Add all fuzz tests under the fuzztest.mcbp_fuzz_test prefix.
    # To run the fuzz tests, the FLAGS_fuzz_for env variable must be set.
    gtest_discover_tests(mcbp_fuzz_test
        EXTRA_ARGS --fromenv=fuzz_for
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        TEST_PREFIX fuzztest.mcbp_fuzz_test.
        DISCOVERY_TIMEOUT 60)
endif()

cb_add_test_executable(mcbp_network_fuzz_test mcbp_network_fuzz_test.cc)
add_sanitizers(mcbp_network_fuzz_test)
target_link_libraries(mcbp_network_fuzz_test PUBLIC mc_client_connection mcbp platform)

if (CB_LIBFUZZER)
    target_compile_definitions(mcbp_network_fuzz_test PUBLIC HAVE_LIBFUZZER=1)
    cb_link_libfuzzer_main(mcbp_network_fuzz_test)
endif ()
