cb_add_test_executable(cluster_test
               clustertest.cc
               clustertest.h
               collection_tests.cc
               collection_dcp_tests.cc
               durability_tests.cc
               durability_upgrade_tests.cc
               main.cc
               misc_tests.cc
               out_of_order_tests.cc
               pitr_tests.cc
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
