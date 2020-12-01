add_library(statistics SHARED
        collector.cc
        cbstat_collector.cc
        definitions.cc
        labelled_collector.cc
        prometheus.cc
        prometheus_collector.cc
        )

add_sanitizers(statistics)

add_dependencies(statistics generated_source_files)

target_include_directories(statistics
        PUBLIC ${PROMETHEUS_INCLUDE_DIR}
               ${Memcached_BINARY_DIR}/engines/ep/src)

target_link_libraries(statistics mcd_util ${PROMETHEUS_LIBRARIES})

GENERATE_EXPORT_HEADER(statistics
        EXPORT_MACRO_NAME STATISTICS_PUBLIC_API
        EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/statistics/visibility.h)

INSTALL(TARGETS statistics
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
