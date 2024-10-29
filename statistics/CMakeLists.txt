add_executable(genstats gen_stat_definitions.cc)
target_link_libraries(genstats PRIVATE
        nlohmann_json::nlohmann_json
        fmt::fmt
        prometheus-cpp::core
        Folly::folly
        platform)
kv_enable_pch(genstats)

if (WIN32)
    # windows need getopt
    target_link_libraries(genstats PRIVATE platform)
endif()

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc
               ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.h
               ${CMAKE_CURRENT_BINARY_DIR}/metrics_metadata.json
        COMMAND
            ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/src
        COMMAND
            genstats -j ${CMAKE_CURRENT_SOURCE_DIR}/stat_definitions.json
                     -C ${Memcached_SOURCE_DIR}/engines/ep/configuration.json
                     -d ${CMAKE_CURRENT_BINARY_DIR}/metrics_metadata.json.temp
                     -h ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.h.temp
                     -c ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc.temp
        COMMAND
            ${CMAKE_COMMAND} -E copy_if_different
                ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.h.temp
                ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.h
        COMMAND
            ${CMAKE_COMMAND} -E copy_if_different
                ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc.temp
                ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc
        COMMAND
            ${CMAKE_COMMAND} -E copy_if_different
                ${CMAKE_CURRENT_BINARY_DIR}/metrics_metadata.json.temp
                ${CMAKE_CURRENT_BINARY_DIR}/metrics_metadata.json
        DEPENDS
            ${Memcached_SOURCE_DIR}/engines/ep/configuration.json
            stat_definitions.json
            genstats
        COMMENT "Generating stat definitions from json")

AddMetricsMetadata(
        JSON "${CMAKE_CURRENT_BINARY_DIR}/metrics_metadata.json"
        COMPONENT kv)

add_custom_target(generated_stats_source_files
        DEPENDS
        ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc
        ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.h)

add_library(statistics STATIC
            collector.cc
            cbstat_collector.cc
            labelled_collector.cc
            prometheus.cc
            prometheus_collector.cc
            statdef.cc
            ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc
            )
kv_enable_pch(statistics)
add_sanitizers(statistics)
add_dependencies(statistics generated_stats_source_files)

target_include_directories(statistics PUBLIC
        ${Memcached_BINARY_DIR}/engines/ep/src
        ${CMAKE_CURRENT_BINARY_DIR}/src/)

target_link_libraries(statistics
        PUBLIC
                platform
                prometheus-cpp::core # ep-engine and core need the include path
        PRIVATE
                phosphor
                hdrhistogram
                Folly::headers
                mcd_util
                OpenSSL::SSL
                spdlog::spdlog
                prometheus-cpp::pull
                )
