add_library(cluster_framework STATIC
            auth_provider_service.cc auth_provider_service.h
            bucket.cc bucket.h
            cluster.cc cluster.h
            dcppipe.cc dcppipe.h
            dcp_packet_filter.h
            dcp_replicator.cc dcp_replicator.h
            node.cc node.h)
target_include_directories(cluster_framework PRIVATE SYSTEM
                           ${FOLLY_INCLUDE_DIR})
target_link_libraries(cluster_framework PUBLIC
                      mc_client_connection
                      dirutils
                      mcd_test_util
                      platform
                      gtest
                      ${FOLLY_LIBRARIES})

