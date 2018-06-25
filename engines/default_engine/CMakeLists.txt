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

SET_TARGET_PROPERTIES(default_engine PROPERTIES PREFIX "")

TARGET_LINK_LIBRARIES(default_engine memcached_logger engine_utilities mcd_util platform ${COUCHBASE_NETWORK_LIBS})

INSTALL(TARGETS default_engine
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    if (NOT WIN32)
        include_directories(AFTER
                            ${benchmark_SOURCE_DIR}/include)

        add_executable(assoc_bench_test
                       assoc.h
                       assoc.cc
                       assoc_bench_test.cc)
        target_link_libraries(assoc_bench_test default_engine benchmark platform)
    endif (NOT WIN32)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
