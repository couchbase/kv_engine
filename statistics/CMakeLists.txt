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
        PUBLIC ${Memcached_BINARY_DIR}/engines/ep/src)

target_link_libraries(statistics
        PUBLIC
                platform
                prometheus-cpp::core # ep-engine and core need the include path
        PRIVATE
                Folly::headers
                mcd_util
                spdlog
                prometheus-cpp::pull
                )
