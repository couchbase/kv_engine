ADD_LIBRARY(default_engine STATIC
            assoc.cc
            assoc.h
            default_engine.cc
            default_engine_internal.h
            engine_manager.cc
            engine_manager.h
            items.cc
            items.h
            scrubber_task.cc
            scrubber_task.h
            slabs.cc
            slabs.h)
kv_enable_pch(default_engine)
cb_enable_unity_build(default_engine)

target_link_libraries(default_engine PRIVATE
        memcached_logger
        engine_utilities
        mcbp
        mcd_util
        platform
        statistics
        ${COUCHBASE_NETWORK_LIBS})
add_sanitizers(default_engine)
