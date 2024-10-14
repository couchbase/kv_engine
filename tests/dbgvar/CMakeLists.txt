# Diable while upgrading ASAN job to clang-15 (and gcc 13.2.0)
if (NOT DEFINED CB_ADDRESSSANITIZER)
    cb_add_test_executable(memcached_dbgvar_test
            dbgvar_test.cc)
    target_link_libraries(memcached_dbgvar_test
            PRIVATE Folly::headers GTest::gtest GTest::gtest_main)
    add_test(NAME memcached_dbgvar_test
            WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
            COMMAND memcached_dbgvar_test)
endif ()
