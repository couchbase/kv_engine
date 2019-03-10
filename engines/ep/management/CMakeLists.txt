# Install the individual commands
SET (KV_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/../../..")
PyWrapper(
    BUILD_DIR "${KV_BINARY_DIR}"
    SCRIPTS
    cbepctl
    cbstats
    cbcompact
    cbvdiff
    cbvbucketctl
    kv_trace_dump)

# Install supporting python files
INSTALL(FILES
        clitool.py
        cli_auth_utils.py
        mc_bin_client.py
        mc_bin_server.py
        memcacheConstants.py
        DESTINATION lib/python)

if (NOT EP_BYPASS_TESTS)
    IF (WIN32)
        SET (ext .bat)
    ELSE ()
        SET (ext)
    ENDIF ()
    # runs cbstats & cbepctl - will fail on basic syntax errors
    ADD_TEST(NAME cbstats_test
             COMMAND "${KV_BINARY_DIR}/cbstats${ext}" --help)
    ADD_TEST(NAME cbepctl_test
             COMMAND "${KV_BINARY_DIR}/cbepctl${ext}" --help)
    ADD_TEST(NAME kv_trace_dump_test
             COMMAND "${KV_BINARY_DIR}/kv_trace_dump${ext}" --help)
endif (NOT EP_BYPASS_TESTS)

INSTALL(PROGRAMS
        cbanalyze-core
        DESTINATION bin)
