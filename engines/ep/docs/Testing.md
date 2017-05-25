# EP-Engine Testing Infrastructure

Test for ep-engine live under the [`tests/`](../tests/)
subdirectory. There are a number of different test types and and
different frameworks - some home-grown, some using standard frameworks
(e.g. GoogleTest/GoogleMock).

## Unit tests

Unit tests are located in
[`tests/module_tests/`](../tests/module_tests). Each file typically
tests a specific class, and the test filename is named `XXX_test.cc`,
where `XXX` is the name of the file containing the unit under test -
for example
[`string_utils_test.cc`](../tests/module_tests/string_utils_test.cc) tests the
functionality in [`string_utils.cc`](../src/string_utils.cc).

These tests initialy used simple, homegrown test framework / asserts,
but newer tests use the
[GoogleTest](https://github.com/google/googletest) framework. As of
writing most (but not all) unit tests have been converted to use
GoogleTest.

Worthy of specific mention are the more complex or advanced 'unit'
test files:

### `evp_store_test` - SynchronousEPEngine

[`evp_store_test.h`](../tests/module_tests/evp_store_test.h) contains
tests and infrastructure for running a _partially_ synchronous variant
of `EventuallyPersistentEngine`, using the `SynchronousEPEngine`
subclass. Its main purpose is to allow us to construct and setup an
EPStore without starting all the various background tasks which are
normally started by EPEngine as part of creating EPStore (in the
`initialize()` method).

The net result is a (mostly) synchronous environment - while the
ExecutorPool's threads exist, none of the normally-created background
Tasks should be running. Note however that _if_ any new tasks are
created, they will be scheduled on the ExecutorPools' threads
asynchronously.

This class (and the associated testcases) give more control over the
execution environment - they allow tests to be written where fewer
asynchronous background tasks run. This simplifies the task of writing
deterministic testcases in what is normally a multi-threaded
environment.

For example, when performing a `get` operation on a
non-resident item, this would normally result in an BGFetch task being
scheduled on a background thread, which may be undesirable when trying
to test access to non-resident items. With `SynchronousEPEngine`, no
BGFetch task will be automatically scheduled, it will only occur when
explicitly run in the test.

### `evp_store_single_threaded_test` - SingleThreadedEPStoreTest

[`evp_store_single_threaded_test.cc`](../tests/module_tests/evp_store_single_threaded_test.cc)
builds on the ideas from `evp_store_test`, creating a _fully_
synchronous test execution environment. Unlike `evp_store_test`, where
the normal background thread pool (`ExecutorPool`) does exist, in
`SingleThreadedEPStoreTest` this is replaced with a
[`SingleThreadedExecutorPool`](../src/fakes/fake_executorpool.h). This
is a class which implements the same API as `ExecutorPool` - Tasks can
be queued in various task queues - however _no_ background threads are
created. The only way any Tasks are executed is if the user explicitly
runs them.

This class gives complete control of the execution of normally
background tasks. This can be useful for bug regression tests (say
when a background task runs at a particular time relative to other
tasks), or for testing interactions between different tasks.

To execute a task, the user must explicitly drive the ExecutorPool,
for example using `runNextTask`:

```C++
// Obtain a reference to a particular task queue - e.g. writer
auto& lpWriterQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];
// Run the next task (synchronously in the current thread).
runNextTask(lpWriterQ);
```

## Component tests

Component tests are located directly in [`tests/`](../tests/). These
test the final, production `ep.so` engine by loading it into a fake
server program (`memcached_testapp`) and then running a series of
testcases against it. For each testcase a 'fresh' instance of
ep-engine is created, and then the testcase drives ep.so via the
public, external interface (`ENGINE_HANDLE_V1`). This is essentially a
Couchbase-grown test harness and framework.

The following component-level testsuites exist:

* [`ep_testsuite_basic`](../tests/ep_testsuite_basic.cc) - Contains
  testcases for 'basic' ep-engine functionality, such as data access
  commands (get, set, incr, etc).

* [`ep_testsuite_checkpoint`](../tests/ep_testsuite_checkpoint.cc) -
  Contains testcases for checkpoint functionality.

* [`ep_testsuite_dcp`](../tests/ep_testsuite_dcpt.cc) - Contains
  testcases for the Data Change Protocol (DCP) api for moving data
  into / out of ep-engine.

* [`ep_testsuite_tap`](../tests/ep_testsuite_tap.cc) -
  Contains testcases for TAP functionality

* [`ep_testsuite_xdcr`](../tests/ep_testsuite_xdcr.cc) -
  Contains testcases for cross-datacentre replicaton APIs.

* [`ep_testsuite`](../tests/ep_testsuite.cc) - The original
  component-level testsuite - originally all the different
  ep_testsuite_XXX suites were rolled into here. Now contains
  miscellaneous tests, and regression tests for bugs (MBs).

* [`ep_testsuite_xdcr`](../tests/ep_testsuite_xdcr.cc) -
  Contains testcases for cross-datacentre replicaton APIs.

* [`ep_perfsuite`](../tests/ep_perfsuite.cc) - Contains performance
  tests for specific, microbenchmark-level functionality -
  e.g. Get/Set/Delete latencies under various scenarios.
