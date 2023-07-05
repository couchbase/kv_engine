add_subdirectory(mcbp)

add_library(memcached_daemon_protocol_utils STATIC
        item_dissector.cc item_dissector.h)
target_link_libraries(memcached_daemon_protocol_utils
        PRIVATE
        hdrhistogram
        memcached_logger
        platform
        cbcompress)
target_include_directories(memcached_daemon_protocol_utils
        INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
