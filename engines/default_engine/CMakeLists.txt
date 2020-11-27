ADD_LIBRARY(default_engine SHARED
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

TARGET_LINK_LIBRARIES(default_engine
        memcached_logger
        engine_utilities
        mcbp
        mcd_util
        platform
        statistics
        ${COUCHBASE_NETWORK_LIBS})
add_sanitizers(default_engine)

INSTALL(TARGETS default_engine
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
