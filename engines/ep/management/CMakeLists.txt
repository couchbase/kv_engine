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
    kv_trace_dump
    kv_slow_ops_2_gtrace)

# Install supporting python files
INSTALL(FILES
        cbstats.py
        clitool.py
        cli_auth_utils.py
        mc_bin_client.py
        mc_bin_server.py
        memcacheConstants.py
        DESTINATION lib/python)

find_package(Python3 COMPONENTS Interpreter)
ADD_TEST(NAME cbstats_py_test
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/cbstats_test.py)

ADD_TEST(NAME kv_slow_ops_2_gtrace_test
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/kv_slow_ops_2_gtrace -v)
set_property(TEST kv_slow_ops_2_gtrace_test PROPERTY ENVIRONMENT "UNITTEST=1")
# runs cbstats & cbepctl - will fail on basic syntax errors
ADD_TEST(NAME cbstats_test
         COMMAND ${KV_BINARY_DIR}/cbstats --help)
ADD_TEST(NAME cbepctl_test
         COMMAND ${KV_BINARY_DIR}/cbepctl --help)
ADD_TEST(NAME kv_trace_dump_test
         COMMAND ${KV_BINARY_DIR}/kv_trace_dump --help)
ADD_TEST(NAME kv_stats_log_test
         COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/kv_stats_log_test.py)

INSTALL(PROGRAMS
        cbanalyze-core
        DESTINATION bin)
