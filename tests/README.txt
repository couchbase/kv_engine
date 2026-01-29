Running Unit Tests for kv_engine
=================================

The kv_engine tests reside in kv_engine/tests and use CMake/CTest for test
execution.

For general build instructions, see:
https://github.com/couchbase/tlm?tab=readme-ov-file#how-to-build

Prerequisites
-------------
1. Ensure the source tree is synced using repo:
      repo sync

2. The build directory should be at the same level as the source (e.g., ../build)

3. ninja build system installed (used by default)

=============================================================================
COMPLETE STEPS TO BUILD AND RUN ALL TESTS
=============================================================================

Step 1: Sync and Build the Project
----------------------------------
From the top-level source directory:

   repo sync
   ./Build.sh

This will configure, build, and install the project to the 'install' directory.

Build.sh Options:
  -s source_root  Source directory (default: current working directory)
  -b build_root   Build directory (default: <cwd>/build)
  -i install_root Install directory (default: <cwd>/install)
  -X              Set Mac platform to x86_64 (for arm64 Macs cross-compiling)
  -T              Enable thread sanitizer
  -A              Enable address sanitizer
  -U              Enable undefined behavior sanitizer
  -R              Set build type to RelWithDebInfo

Step 2: Build Test Executables
------------------------------
Test executables are NOT built by default. The simplest way to build
everything for kv_engine (including all test programs) is:

   cd <build_directory>
   ninja kv_engine_everything

Step 3: Run All Tests
---------------------
   cd <build_directory>/kv_engine
   ctest --output-on-failure

Alternatively, you can build and run all tests in one step from the build
directory (output goes to a file rather than console):

   ninja kv_engine/test

=============================================================================
DEBUG MODE
=============================================================================

To build in Debug mode (full debug symbols, no optimization):

Option 1: Using Build.sh environment variable
---------------------------------------------
   CMAKE_BUILD_TYPE=Debug ./Build.sh

Option 2: Manual CMake configuration
------------------------------------
   cd <build_directory>
   cmake -DCMAKE_BUILD_TYPE=Debug ..
   ninja

Available build types:
  - Debug:          Full debug symbols, no optimization (-O0 -g)
  - DebugOptimized: Debug symbols with optimization (-Og -g) [default]
  - Release:        Full optimization, no debug symbols
  - RelWithDebInfo: Release with debug info

=============================================================================
macOS ARM64 (Apple Silicon) Notes
=============================================================================

For arm64 Macs, if you encounter "'to_chars' is unavailable" errors, ensure
the deployment target is set correctly:

   cd <build_directory>
   cmake -DCMAKE_BUILD_TYPE=Debug -DCB_OVERRIDE_OSX_DEPLOYMENT_TARGET=13.3 ..

After changing the deployment target, clean precompiled headers:

   find . -name "*.pch" -delete

Then rebuild:

   ninja

=============================================================================
CTEST COMMANDS
=============================================================================

Run all tests with output on failure:
   ctest --output-on-failure

Run tests with verbose output:
   ctest -V

Run a specific test by name (using regex):
   ctest -R <test-name-pattern>

List all available tests without running them:
   ctest -N

Re-run only failed tests:
   ctest --rerun-failed --output-on-failure

Run tests in parallel (e.g., 4 jobs):
   ctest -j4 --output-on-failure

Stop on first failure:
   ctest --stop-on-failure --output-on-failure

Run tests from top-level build directory:
   ctest --test-dir kv_engine --output-on-failure

=============================================================================
TROUBLESHOOTING
=============================================================================

1. Tests show "Not Run" status
   CAUSE: Test executables were not built
   FIX: Run "ninja kv_engine_everything" to build all test executables

2. Precompiled header errors after changing build settings
   CAUSE: Old .pch files compiled with different settings
   FIX: Run "find . -name '*.pch' -delete" and rebuild

3. To list all available test targets:
   cd <build_directory> && ninja -t targets | grep -E "_test|testsuite"

4. Build failures due to missing headers after repo sync
   FIX: Ensure all repos are properly synced:
        repo sync
   If conflicts exist, check with:
        repo status

=============================================================================
RUNNING INDIVIDUAL TEST BINARIES DIRECTLY
=============================================================================

For more control or debugging, you can run test binaries directly.
Running a test binary without arguments runs all tests in that binary.

   cd <build_directory>/kv_engine

   # Run all tests in memcached_unit_tests
   ./memcached_unit_tests

   # Run EP-engine unit tests with a filter
   ./ep-engine_ep_unit_tests --gtest_filter="*CheckpointTest*"

   # List all available tests
   ./memcached_unit_tests --gtest_list_tests

GoogleTest options (for *_test binaries):
  --gtest_filter=PATTERN    Run only tests matching the pattern (omit to run all)
  --gtest_list_tests        List all test names
  --gtest_repeat=N          Run tests N times
  --gtest_shuffle           Randomize test order
  --gtest_output=xml:FILE   Output results to XML file
