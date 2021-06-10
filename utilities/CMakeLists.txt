if ("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
    # Breakpad has headers (which are dependencies for our breakpad integration
    # code) that don't compile cleanly as C++11, so the GNU standard is
    # used instead.
    # This also means precompiled headers must be disabled given
    # the compile flags differ from the other source files.
    set_source_files_properties(breakpad.cc PROPERTIES
        COMPILE_FLAGS ${CB_GNU_CXX11_OPTION}
        SKIP_PRECOMPILE_HEADERS ON)
endif ()

# Mark third-party libraries as 'system' so we skip any warnings they
# generate.
INCLUDE_DIRECTORIES(BEFORE SYSTEM ${hdr_histogram_SOURCE_DIR}/src)

add_library(mcd_util STATIC
            ${PROJECT_SOURCE_DIR}/include/memcached/config_parser.h
            ${PROJECT_SOURCE_DIR}/include/memcached/vbucket.h
            ${PROJECT_SOURCE_DIR}/include/memcached/util.h
            breakpad.h
            breakpad.cc
            breakpad_settings.cc
            breakpad_settings.h
            terminal_color.cc
            terminal_color.h
            config_parser.cc
            dcp_stream_id.cc
            dockey.cc
            durability_spec.cc
            engine_errc_2_mcbp.cc
            engine_errc_2_mcbp.h
            hdrhistogram.cc
            hdrhistogram.h
            json_utilities.cc
            json_utilities.h
            logtags.cc
            logtags.h
            openssl_utils.cc
            openssl_utils.h
            string_utilities.cc
            string_utilities.h
            terminate_handler.cc
            terminate_handler.h
            thread_pool_config.cc
            types.cc
            util.cc
            vbucket.cc )
set_property(TARGET mcd_util PROPERTY POSITION_INDEPENDENT_CODE 1)
cb_enable_unity_build(mcd_util)

if (BREAKPAD_FOUND)
  target_include_directories(mcd_util SYSTEM PRIVATE ${BREAKPAD_INCLUDE_DIR})
  target_link_libraries(mcd_util PRIVATE ${BREAKPAD_LIBRARIES})
endif()
target_link_libraries(mcd_util
        PUBLIC
                hdr_histogram_static
        PRIVATE
                engine_utilities
                memcached_logger
                platform
                ${OPENSSL_LIBRARIES})
add_sanitizers(mcd_util)
kv_enable_pch(mcd_util)

add_library(mcd_test_util STATIC test_manifest.h test_manifest.cc)
set_property(TARGET mcd_test_util PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mcd_test_util
        PUBLIC spdlog::spdlog
        PRIVATE platform_headers nlohmann_json::nlohmann_json)
add_sanitizers(mcd_test_util)
kv_enable_pch(mcd_test_util)

cb_add_test_executable(utilities_testapp util_test.cc)
kv_enable_pch(utilities_testapp)
target_link_libraries(utilities_testapp PRIVATE
                      mcd_util
                      platform
                      GTest::gtest
                      GTest::gtest_main
                      GTest::gmock)
add_sanitizers(utilities_testapp)
add_test(NAME memcached-utilities-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND utilities_testapp)
