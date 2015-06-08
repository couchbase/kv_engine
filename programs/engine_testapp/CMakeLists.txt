ADD_EXECUTABLE(engine_testapp engine_testapp.c
                              mock_server.c
                              mock_server.h
                              ${MEMORY_TRACKING_SRCS})
TARGET_LINK_LIBRARIES(engine_testapp mcd_util platform
                      ${MALLOC_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
APPEND_MALLOC_LINKER_FLAGS(engine_testapp)
INSTALL(TARGETS engine_testapp
        RUNTIME DESTINATION bin)

ADD_TEST(NAME memcached-basic-engine-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND engine_testapp -E default_engine.so
	                        -T basic_engine_testsuite.so)
