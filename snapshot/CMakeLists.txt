add_library(snapshot STATIC
        download_properties.cc
        download_properties.h
        file_downloader.cc
        file_downloader.h
        manifest.h
        manifest.cc
        snapshot_downloader.cc
        snapshot_downloader.h
)
kv_enable_pch(snapshot)
cb_enable_unity_build(snapshot)
target_link_libraries(snapshot PUBLIC
        mc_client_connection
        platform
        spdlog::spdlog
        statistics)
add_sanitizers(snapshot)

cb_add_test_executable(snapshot_tests
        download_properties_test.cc
        file_downloader_test.cc
        manifest_test.cc)
kv_enable_pch(snapshot_tests)
cb_enable_unity_build(snapshot_tests)
target_link_libraries(snapshot_tests
        PUBLIC
        snapshot
        mc_client_connection
        GTest::gtest
        GTest::gtest_main)
add_sanitizers(snapshot_tests)
add_test(NAME snapshot_tests
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND snapshot_tests)
