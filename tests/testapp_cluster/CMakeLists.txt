cb_add_test_executable(cluster_test
               clustertest.cc
               clustertest.h
               collection_tests.cc
               collection_dcp_tests.cc
               cross_bucket_quota_sharing_test.cc
               durability_tests.cc
               durability_upgrade_tests.cc
               get_ex_tests.cc
               get_keys_test.cc
               main.cc
               misc_tests.cc
               out_of_order_tests.cc
               quota_sharing_paging_test.cc
               rbac_tests.cc
               regression_tests.cc
               snapshot_tests.cc
               upgrade_test.cc
               upgrade_test.h)
target_link_libraries(cluster_test PRIVATE cluster_framework snapshot)
add_dependencies(cluster_test memcached Generate_kv_engine_X509_Test_Certificates)
add_sanitizers(cluster_test)
cb_enable_unity_build(cluster_test)
kv_enable_pch(cluster_test)
add_test(NAME cluster_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cluster_test)
# The cluster_test spins up 4 memcached instances and is relatively heavyweight.
# Don't run it concurrently with any other tests.
set_tests_properties(cluster_test
        PROPERTIES RUN_SERIAL TRUE)

if (BUILD_ENTERPRISE AND NOT WIN32)
    # Magma emits a lot of critical warnings and fails on
    # win32 due to failing to open files.
    # Only run on other platforms for now
    add_test(NAME cluster_test_magma
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND cluster_test --backend=magma)
    # The cluster_test spins up 4 memcached instances and is relatively heavyweight.
    # Don't run it concurrently with any other tests.
    set_tests_properties(cluster_test_magma PROPERTIES RUN_SERIAL TRUE)
endif()
