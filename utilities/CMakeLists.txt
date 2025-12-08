if ("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
endif ()

add_library(mcd_util STATIC
            ${PROJECT_SOURCE_DIR}/include/memcached/configuration_iface.h
            ${PROJECT_SOURCE_DIR}/include/memcached/config_parser.h
            ${PROJECT_SOURCE_DIR}/include/memcached/vbucket.h
            ${PROJECT_SOURCE_DIR}/include/memcached/util.h
            breakpad.h
            breakpad_common.cc
            breakpad_settings.cc
            breakpad_settings.h
            configuration_iface.cc
            config_parser.cc
            connection_iface.cc
            cookie_iface.cc
            dcp_stream_id.cc
            dek_file_utilities.cc
            dek_file_utilities.h
            dockey_view.cc
            durability_spec.cc
            engine_errc_2_mcbp.cc
            engine_errc_2_mcbp.h
            engine_storage.cc
            fusion_utilities.h
            global_concurrency_semaphores.cc
            global_concurrency_semaphores.h
            io_control.cc
            json_utilities.cc
            json_utilities.h
            lock_utilities.h
            logtags.cc
            logtags.h
            serverless_config.cc
            stat_group.cc
            string_utilities.cc
            string_utilities.h
            storeddockey.cc
            terminate_handler.cc
            terminate_handler.h
            thread_pool_config.cc
            throttle_utilities.cc
            throttle_utilities.h
            timing_histogram_printer.cc
            timing_histogram_printer.h
            types.cc
            util.cc
            vbucket.cc )
set_property(TARGET mcd_util PROPERTY POSITION_INDEPENDENT_CODE 1)
cb_enable_unity_build(mcd_util)

if (APPLE)
    target_sources(mcd_util PRIVATE breakpad_not_supported.cc)
else ()
    if (WIN32)
        target_sources(mcd_util PRIVATE breakpad_win32.cc)
        # make sure we build breakpad_win32.cc outside of unity builds
        # and without including precompiled headers (as it may include
        # more headers we don't want)
        set_source_files_properties(breakpad_win32.cc PROPERTIES
                SKIP_UNITY_BUILD_INCLUSION TRUE
                SKIP_PRECOMPILE_HEADERS TRUE)
    else ()
        # Breakpad has headers (which are dependencies for our breakpad
        # integration code) that don't compile cleanly as C++11, so the
        # GNU standard is used instead.
        # This also means precompiled headers must be disabled given
        # the compile flags differ from the other source files.
        set_source_files_properties(breakpad_linux.cc PROPERTIES
                COMPILE_FLAGS ${CB_GNU_CXX11_OPTION}
                SKIP_PRECOMPILE_HEADERS TRUE
                SKIP_UNITY_BUILD_INCLUSION TRUE)
        target_sources(mcd_util PRIVATE breakpad_linux.cc)
    endif ()

    target_include_directories(mcd_util SYSTEM PRIVATE ${BREAKPAD_INCLUDE_DIR})
    target_link_libraries(mcd_util PRIVATE ${BREAKPAD_LIBRARIES})
endif()

target_link_libraries(mcd_util
        PUBLIC
                platform
        PRIVATE
                engine_utilities
                memcached_logger
                hdrhistogram
                OpenSSL::SSL)
add_sanitizers(mcd_util)
kv_enable_pch(mcd_util)

add_library(mcd_test_util STATIC test_manifest.h test_manifest.cc)
set_property(TARGET mcd_test_util PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mcd_test_util
        PUBLIC spdlog::spdlog
        PRIVATE platform_headers nlohmann_json::nlohmann_json)
add_sanitizers(mcd_test_util)
kv_enable_pch(mcd_test_util)

cb_add_test_executable(utilities_testapp
        breakpad_settings_test.cc
        config_parser_test.cc
        dek_file_utilities_test.cc
        stat_group_test.cc
        string_utilities_test.cc
        throttle_utilities_test.cc
        util_test.cc)
kv_enable_pch(utilities_testapp)
cb_enable_unity_build(utilities_testapp)
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

cb_add_test_executable(string_utilities_benchmark
        string_utilities_benchmark.cc)
target_link_libraries(string_utilities_benchmark PRIVATE
        benchmark::benchmark mcd_util)
kv_enable_pch(string_utilities_benchmark)
add_sanitizers(string_utilities_benchmark)

add_library(magma_support OBJECT magma_support.h magma_support.cc)
if (TARGET magma)
    target_compile_definitions(magma_support PUBLIC HAVE_MAGMA_SUPPORT=1)
    target_link_libraries(magma_support PUBLIC magma)
    if (TARGET fusionfs)
        target_compile_definitions(magma_support PUBLIC HAVE_FUSION_SUPPORT=1)
    endif ()
else()
    target_link_libraries(magma_support PUBLIC nlohmann_json::nlohmann_json)
endif ()
