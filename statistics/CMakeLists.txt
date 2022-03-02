add_executable(genstats gen_stat_definitions.cc)
target_link_libraries(genstats PRIVATE nlohmann_json::nlohmann_json fmt::fmt)
kv_enable_pch(genstats)

if (WIN32)
    # windows need getopt
    target_link_libraries(genstats PRIVATE platform)
endif()

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc
               ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.h
        COMMAND
            ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/src
        COMMAND
            genstats -j ${CMAKE_CURRENT_SOURCE_DIR}/stat_definitions.json
                     -C ${Memcached_SOURCE_DIR}/engines/ep/configuration.json
                     -h ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.h
                     -c ${CMAKE_CURRENT_BINARY_DIR}/src/generated_stats.cc
        DEPENDS
            stat_definitions.json
            genstats
        COMMENT "Generating stat definitions from json")

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
                hdrhistogram
                Folly::headers
                mcd_util
                spdlog::spdlog
                prometheus-cpp::pull
                )
