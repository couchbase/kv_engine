# Initialize working python virtual environment for this directory
PyVenv()

# Install the individual commands
PyInstall(TARGET cbcompact SCRIPT cbcompact INSTALL_PATH bin SYMLINK_DIR ${CMAKE_BINARY_DIR}/kv_engine)
PyInstall(TARGET cbepctl SCRIPT cbepctl INSTALL_PATH bin SYMLINK_DIR ${CMAKE_BINARY_DIR}/kv_engine)
PyInstall(TARGET cbstats SCRIPT cbstats INSTALL_PATH bin SYMLINK_DIR ${CMAKE_BINARY_DIR}/kv_engine)
PyInstall(TARGET cbvbucketctl SCRIPT cbvbucketctl INSTALL_PATH bin SYMLINK_DIR ${CMAKE_BINARY_DIR}/kv_engine)
PyInstall(TARGET cbvdiff SCRIPT cbvdiff INSTALL_PATH bin SYMLINK_DIR ${CMAKE_BINARY_DIR}/kv_engine)
PyInstall(TARGET kv_trace_dump SCRIPT kv_trace_dump INSTALL_PATH bin SYMLINK_DIR ${CMAKE_BINARY_DIR}/kv_engine)

if (NOT EP_BYPASS_TESTS)
    # runs cbstats & cbepctl - will fail on basic syntax errors
    ADD_TEST(NAME cbstats_test
             COMMAND ${CMAKE_INSTALL_PREFIX}/bin/cbstats --help)
    ADD_TEST(NAME cbepctl_test
             COMMAND ${CMAKE_INSTALL_PREFIX}/bin/cbepctl --help)
    ADD_TEST(NAME kv_trace_dump_test
             COMMAND ${CMAKE_INSTALL_PREFIX}/bin/kv_trace_dump --help)
endif (NOT EP_BYPASS_TESTS)

INSTALL(PROGRAMS
        cbanalyze-core
        DESTINATION bin)
