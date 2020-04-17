add_library(statistics STATIC
        definitions.cc
        collector.cc
        )

SET_PROPERTY(TARGET statistics PROPERTY POSITION_INDEPENDENT_CODE 1)

target_link_libraries(statistics hdr_histogram_static)