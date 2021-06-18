include_directories(AFTER SYSTEM ${gtest_SOURCE_DIR}/include)
cb_add_test_executable(memcached_mcbp_test
               mcbp_frame_extra.cc
               mcbp_dcp_test.cc
               mcbp_gat_test.cc
               mcbp_test.cc
               mcbp_test.h
               mcbp_test_2.cc
               mcbp_test_collections.cc
               mcbp_test_main.cc
               mcbp_test_meta.cc
               mcbp_test_subdoc.cc
               mcbp_test_subdoc_xattr.cc
               mock_connection.h
               set_vbucket_validator_test.cc
               xattr_blob_test.cc
               xattr_blob_validator_test.cc
               xattr_key_validator_test.cc
               ${PROJECT_SOURCE_DIR}/protocol/connection/frameinfo.cc
               ${PROJECT_SOURCE_DIR}/protocol/connection/client_mcbp_commands.cc)
target_link_libraries(memcached_mcbp_test
        PRIVATE
                memcached_daemon
                platform
                gtest)
add_sanitizers(memcached_mcbp_test)
cb_enable_unity_build(memcached_mcbp_test)
kv_enable_pch(memcached_mcbp_test)

add_test(NAME memcached_mcbp_test COMMAND memcached_mcbp_test)

if(MSVC)
    # mcbp_test now too large for MSVC and requires bigobj
    target_compile_options(memcached_mcbp_test PRIVATE "/bigobj")
endif()

cb_add_test_executable(memcached_mcbp_bench
        mcbp_bench.cc)
kv_enable_pch(memcached_mcbp_bench)
target_include_directories(memcached_mcbp_bench
    SYSTEM PRIVATE
    ${benchmark_SOURCE_DIR}/include)
target_link_libraries(memcached_mcbp_bench PRIVATE
                      benchmark memcached_daemon)
add_sanitizers(memcached_mcbp_bench)
