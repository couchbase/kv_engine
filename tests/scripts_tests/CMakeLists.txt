IF(NOT WIN32)
    cb_add_test_executable(jemalloc_allocator_stats
            jemalloc_allocator_stats.cc)
    target_include_directories(jemalloc_allocator_stats
            SYSTEM PRIVATE ${JEMALLOC_INCLUDE_DIR})

    target_link_libraries(jemalloc_allocator_stats
            PRIVATE platform ${JEMALLOC_LIBRARIES})

    ADD_TEST(NAME jemalloc_analyse
                COMMAND ${CMAKE_COMMAND}
                -DSRC_DIR=${CMAKE_CURRENT_SOURCE_DIR}
                -DJEMALLOC_ANALYSE=${PROJECT_SOURCE_DIR}/scripts/jemalloc/jemalloc_analyse.py
                -DTEST_PROGRAM=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/jemalloc_allocator_stats
                -P ${CMAKE_CURRENT_SOURCE_DIR}/runtests.cmake)
ENDIF()
