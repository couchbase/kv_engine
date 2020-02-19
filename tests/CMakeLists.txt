add_compile_options_disable_optimization()

ADD_SUBDIRECTORY(config_parse_test)
ADD_SUBDIRECTORY(datatype)
ADD_SUBDIRECTORY(doc_server_api)
ADD_SUBDIRECTORY(dockey)
ADD_SUBDIRECTORY(engine_error)
ADD_SUBDIRECTORY(error_map_sanity_check)
ADD_SUBDIRECTORY(executor)
ADD_SUBDIRECTORY(function_chain)
ADD_SUBDIRECTORY(histograms)
ADD_SUBDIRECTORY(mc_time)
ADD_SUBDIRECTORY(mcbp)
ADD_SUBDIRECTORY(memory_tracking_test)
ADD_SUBDIRECTORY(saslprep)
ADD_SUBDIRECTORY(scripts_tests)
ADD_SUBDIRECTORY(sizes)
ADD_SUBDIRECTORY(testapp)
add_subdirectory(testapp_cluster)
ADD_SUBDIRECTORY(topkeys)
ADD_SUBDIRECTORY(unsigned_leb128)

add_test(NAME kv-engine-check-header-define-once-guard COMMAND ${PYTHON_EXECUTABLE}
        ${Memcached_SOURCE_DIR}/tests/header_define_once_test.py
        --rootdir ${Memcached_SOURCE_DIR}
        --exclude engines/ep/src/tasks.def.h)
