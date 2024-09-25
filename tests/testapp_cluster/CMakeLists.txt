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
               upgrade_test.cc
               upgrade_test.h)
target_link_libraries(cluster_test PRIVATE cluster_framework)
add_dependencies(cluster_test memcached)
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
