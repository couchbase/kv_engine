add_library(cluster_framework STATIC
            bucket.cc bucket.h
            cluster.cc cluster.h
            clustertest.cc clustertest.h
            dcppipe.cc dcppipe.h
            dcp_packet_filter.h
            dcp_replicator.cc dcp_replicator.h
            node.cc node.h)
target_include_directories(cluster_framework PRIVATE SYSTEM
                           ${FOLLY_INCLUDE_DIR})
target_link_libraries(cluster_framework PUBLIC
                      mc_client_connection
                      dirutils
                      platform
                      gtest
                      ${FOLLY_LIBRARIES})

add_executable(cluster_test
               collection_tests.cc
               durability_tests.cc
               durability_upgrade_tests.cc
               main.cc
               out_of_order_tests.cc)
target_link_libraries(cluster_test cluster_framework)
add_dependencies(cluster_test memcached ep default_engine)
add_sanitizers(cluster_test)

add_test(NAME cluster_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cluster_test)
