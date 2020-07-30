add_executable(cluster_test
               clustertest.cc
               clustertest.h
               collection_tests.cc
               collection_dcp_tests.cc
               durability_tests.cc
               durability_upgrade_tests.cc
               main.cc
               misc_tests.cc
               out_of_order_tests.cc
               rbac_tests.cc)
target_link_libraries(cluster_test cluster_framework)
add_dependencies(cluster_test memcached ep default_engine)
add_sanitizers(cluster_test)

add_test(NAME cluster_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cluster_test)
