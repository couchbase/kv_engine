add_library(statistics STATIC
            collector.cc
            cbstat_collector.cc
            definitions.cc
            labelled_collector.cc
            prometheus.cc
            prometheus_collector.cc
            )
kv_enable_pch(statistics)

add_sanitizers(statistics)

add_dependencies(statistics generated_source_files)

target_include_directories(statistics
        PUBLIC ${PROMETHEUS_INCLUDE_DIR}
               ${Memcached_BINARY_DIR}/engines/ep/src)

target_link_libraries(statistics
        PUBLIC
                platform
        PRIVATE
                Folly::headers
                mcd_util
                spdlog
                ${PROMETHEUS_LIBRARIES})
