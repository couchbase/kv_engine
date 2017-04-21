FILE(GLOB SAMPLE_ALLOCATOR_STATS
        ${SRC_DIR}/jemalloc_*.log
        )
FILE(COPY ${SAMPLE_ALLOCATOR_STATS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

IF("${COUCHBASE_MEMORY_ALLOCATOR}" STREQUAL "jemalloc")
        EXECUTE_PROCESS(
                COMMAND ${TEST_PROGRAM}
                ERROR_FILE ${CMAKE_CURRENT_BINARY_DIR}/jemalloc_current.log
        )
ENDIF()

FOREACH(version 3.6 4.3.1 current)
        SET(INPUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/jemalloc_${version}.log)
        SET(OUTPUT_FILE
                ${CMAKE_CURRENT_BINARY_DIR}/jemalloc_output_${version}.log)
        SET(COMPARISON_FILE
                ${SRC_DIR}/jemalloc_analyse_expected_output_${version}.log)

        IF(EXISTS ${INPUT_FILE})
                EXECUTE_PROCESS(
                        COMMAND ${PYTHON_EXECUTABLE} ${JEMALLOC_ANALYSE}
                        INPUT_FILE ${INPUT_FILE}
                        OUTPUT_FILE ${OUTPUT_FILE}
                        )

                EXECUTE_PROCESS(
                        COMMAND ${CMAKE_COMMAND} -E compare_files
                        ${OUTPUT_FILE}
                        ${COMPARISON_FILE}
                        RESULT_VARIABLE result
                        )

                IF(NOT "${result}" STREQUAL "0")
                        message(FATAL_ERROR "jemalloc_analyse output does not "
                                "match expected for version ${version}")
                ENDIF()
        ENDIF()
ENDFOREACH()
