IF(${COUCHBASE_PYTHON})
    FOREACH(ID RANGE 9)
        LIST(APPEND GENERATED_SRCS ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_${ID}.cc)
    ENDFOREACH()

    ADD_CUSTOM_COMMAND(OUTPUT ${GENERATED_SRCS}
                       COMMAND
                          ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/engine_test.py
                       DEPENDS
                           engine_test.py
                           breakdancer.py
                       COMMENT "Generating testsuite")

    # Dummy target to prevent cmake trying to run multiple instances of the
    # above "Generating testsuite" command. The actual shared libraries
    # below in turn depend on this, causing them to be 'blocked' behind this.
    ADD_CUSTOM_TARGET(DUMMY_generated_suite
                      DEPENDS ${GENERATED_SRCS}
                      COMMENT "Waiting for testsuite generation to complete")

    FOREACH(ID RANGE 9)
        ADD_LIBRARY(memcached_generated_testsuite_${ID} MODULE
                    suite_stubs.cc
                    ${CMAKE_CURRENT_BINARY_DIR}/generated_suite_${ID}.cc)
        SET_TARGET_PROPERTIES(memcached_generated_testsuite_${ID} PROPERTIES PREFIX "")
        TARGET_LINK_LIBRARIES(memcached_generated_testsuite_${ID} platform)
        ADD_DEPENDENCIES(memcached_generated_testsuite_${ID}
                         DUMMY_generated_suite)

        ADD_TEST(NAME memcached-breakdancer-engine-tests_${ID}
                 WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
                 COMMAND engine_testapp -E mc
		                     -T memcached_generated_testsuite_${ID}.so)
    ENDFOREACH()
ENDIF(${COUCHBASE_PYTHON})
