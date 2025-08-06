SET (KV_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/../../..")

# Python scripts necessary for cbepctl
SET (cbepctl_files
  clitool.py
  cli_auth_utils.py
  mc_bin_client.py
  memcacheConstants.py
)

# Python scripts necessary for running cbstats - other tools may require
# these as well, that's fine
SET (cbstats_files
  cbstats.py
  clitool.py
  cli_auth_utils.py
  mc_bin_client.py
  memcacheConstants.py
)

# Python script for other tools
SET (py_files
  mc_bin_server.py
)

# Install cbepctl, including adding to admin_tools package
PyWrapper(
  BUILD_DIR "${KV_BINARY_DIR}"
  ADD_TO_STANDALONE_PACKAGE admin_tools
  SCRIPTS
    cbepctl
  EXTRA_SCRIPTS ${cbepctl_files}
)

# Install cbstats, including adding to admin_tools package
PyWrapper(
  BUILD_DIR "${KV_BINARY_DIR}"
  ADD_TO_STANDALONE_PACKAGE admin_tools
  SCRIPTS
    cbstats
  EXTRA_SCRIPTS ${cbstats_files}
)

# Install the other commands, including any additional scripts
PyWrapper(
  BUILD_DIR "${KV_BINARY_DIR}"
  SCRIPTS
    cbcompact
    cbvdiff
    cbvbucketctl
    kv_trace_dump
    kv_slow_ops_2_gtrace
  EXTRA_SCRIPTS ${py_files}
)

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
