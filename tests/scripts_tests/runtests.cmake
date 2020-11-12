FILE(GLOB SAMPLE_ALLOCATOR_STATS
        ${SRC_DIR}/jemalloc_*.log
        )
FILE(COPY ${SAMPLE_ALLOCATOR_STATS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

EXECUTE_PROCESS(
        COMMAND ${TEST_PROGRAM}
        ERROR_FILE ${CMAKE_CURRENT_BINARY_DIR}/jemalloc_current.log
)

FOREACH(version 3.6 3.6_cropped 4.3.1 5.2.1)
        SET(INPUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/jemalloc_${version}.log)
        SET(OUTPUT_FILE
                ${CMAKE_CURRENT_BINARY_DIR}/jemalloc_output_${version}.log)
        SET(COMPARISON_FILE
                ${SRC_DIR}/jemalloc_analyse_expected_output_${version}.log)

        EXECUTE_PROCESS(
                        COMMAND ${PYTHON_EXECUTABLE} ${JEMALLOC_ANALYSE}
                        INPUT_FILE ${INPUT_FILE}
                        OUTPUT_FILE ${OUTPUT_FILE}
                        RESULT_VARIABLE result
                        ERROR_VARIABLE error
        )

        IF(NOT "${result}" STREQUAL "0")
                message(FATAL_ERROR "jemalloc_analyse failed for version ${version}: ${error}")
        ENDIF()

        # For version 'current' we don't have an expected output, so just
        # check that jemalloc_analyse didn't error when parsing.
        IF(NOT version STREQUAL "current")
                EXECUTE_PROCESS(COMMAND diff -u ${OUTPUT_FILE} ${COMPARISON_FILE}
                        RESULT_VARIABLE result
                        OUTPUT_VARIABLE output
                )
                IF(NOT "${result}" STREQUAL "0")
                        message(FATAL_ERROR "jemalloc_analyse output does not "
                                "match expected for version ${version}: ${output}")
                ENDIF()
        ENDIF()
ENDFOREACH()
