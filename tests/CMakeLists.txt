add_compile_options_disable_optimization()

add_subdirectory(cert)
add_subdirectory(dbgvar)
ADD_SUBDIRECTORY(dockey)
ADD_SUBDIRECTORY(engine_error)
add_subdirectory(gocode)
ADD_SUBDIRECTORY(histograms)
ADD_SUBDIRECTORY(mcbp)
ADD_SUBDIRECTORY(scripts_tests)
ADD_SUBDIRECTORY(statistics)
ADD_SUBDIRECTORY(testapp)
add_subdirectory(testapp_cluster)
ADD_SUBDIRECTORY(unsigned_leb128)

add_test(NAME kv-engine-check-header-define-once-guard
        COMMAND check_pragma_once
        --rootdir ${Memcached_SOURCE_DIR}
        --exclude executor/tasks.def.h)
