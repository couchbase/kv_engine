add_library(mcbp STATIC
            ${Memcached_SOURCE_DIR}/include/mcbp/mcbp.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/datatype.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/feature.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/framebuilder.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/magic.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/opcode.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/request.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/response.h
            ${Memcached_SOURCE_DIR}/include/mcbp/protocol/status.h
            datatype.cc
            dcp_snapshot_marker_codec.cc
            dump.cc
            ewb_encode.cc
            feature.cc
            gdb_dump_parser.cc
            lldb_dump_parser.cc
            magic.cc
            opcode.cc
            request.cc
            response.cc
            header.cc
            sla.cc
            status.cc
            )

target_link_libraries(mcbp mcd_util dirutils)
set_property(TARGET mcbp PROPERTY POSITION_INDEPENDENT_CODE 1)

add_executable(mcbp_packet_printer mcbp_packet_printer.cc)
target_link_libraries(mcbp_packet_printer mcbp dirutils)
add_sanitizers(mcbp_packet_printer)
install(TARGETS mcbp_packet_printer RUNTIME DESTINATION bin)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(mcbp_dump_parser_test mcbp_dump_parser_test.cc)
    target_link_libraries(mcbp_dump_parser_test mcbp gtest gtest_main)
    add_test(NAME mcbp_dump_parser_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_dump_parser_test)

    add_executable(mcbp_magic_test magic_test.cc)
    target_link_libraries(mcbp_magic_test mcbp gtest gtest_main)
    add_test(NAME mcbp_magic_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_magic_test)
    add_sanitizers(mcbp_magic_test)

    add_executable(mcbp_opcode_test opcode_test.cc)
    target_link_libraries(mcbp_opcode_test mcbp gtest gtest_main)
    add_test(NAME mcbp_opcode_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_opcode_test)
    add_sanitizers(mcbp_opcode_test)

    add_executable(mcbp_feature_test feature_test.cc)
    target_link_libraries(mcbp_feature_test mcbp gtest gtest_main)
    add_test(NAME mcbp_feature_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_feature_test)
    add_sanitizers(mcbp_feature_test)

    add_executable(mcbp_framebuilder_test framebuilder_test.cc)
    target_link_libraries(mcbp_framebuilder_test mcbp gtest gtest_main)
    add_test(NAME mcbp_framebuilder_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_framebuilder_test)
    add_sanitizers(mcbp_framebuilder_test)

    add_executable(mcbp_sla_test sla_test.cc)
    target_link_libraries(mcbp_sla_test mcbp gtest gtest_main)
    add_test(NAME mcbp_sla_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_sla_test)
    add_sanitizers(mcbp_sla_test)

    add_executable(mcbp_request_test request_test.cc)
    target_link_libraries(mcbp_request_test mcbp gtest gtest_main)
    add_test(NAME mcbp_request_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_request_test)
    add_sanitizers(mcbp_request_test)

    add_executable(mcbp_status_test status_test.cc)
    target_link_libraries(mcbp_status_test mcbp gtest gtest_main)
    add_test(NAME mcbp_status_test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND mcbp_status_test)
    add_sanitizers(mcbp_status_test)
endif ()
