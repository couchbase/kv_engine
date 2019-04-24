add_library(mock_server STATIC
            mock_server.cc
            mock_server.h
            ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.cc
            ${Memcached_SOURCE_DIR}/daemon/protocol/mcbp/engine_errc_2_mcbp.h
            ${Memcached_SOURCE_DIR}/daemon/doc_pre_expiry.cc
            $<TARGET_OBJECTS:memory_tracking>)
set_property(TARGET mock_server PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mock_server INTERFACE
                      memcached_logger
                      mcd_tracing
                      mcd_util
                      platform
                      xattr
		      ${MALLOC_LIBRARIES})

add_executable(engine_testapp engine_testapp.cc)
target_link_libraries(engine_testapp PRIVATE
		phosphor
		platform
		mock_server
		${COUCHBASE_NETWORK_LIBS})

add_sanitizers(engine_testapp)
install(TARGETS engine_testapp
        RUNTIME DESTINATION bin)

add_test(NAME memcached-basic-engine-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND engine_testapp -E default_engine.so
	                        -T basic_engine_testsuite.so)
