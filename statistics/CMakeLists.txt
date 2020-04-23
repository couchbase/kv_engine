add_library(statistics STATIC
        collector.cc
        definitions.cc
        labelled_collector.cc)

SET_PROPERTY(TARGET statistics PROPERTY POSITION_INDEPENDENT_CODE 1)

target_link_libraries(statistics hdr_histogram_static)

add_dependencies(statistics generated_source_files)
target_include_directories(statistics PRIVATE ${Memcached_BINARY_DIR}/engines/ep/src)