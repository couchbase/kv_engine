# Fuzz testing

We have some tests that were added a while back which use libFuzzer without any
test fixture.

Newer tests should use Google's [fuzztest library](https://github.com/google/fuzztest).
The fuzztest library registers the fuzz tests with GTest allows the reuse of
GTest test fixtures.

See also:
- [fuzztest Overview](https://github.com/google/fuzztest/blob/main/doc/overview.md)
- [Google's fuzzing guide](https://github.com/google/fuzzing/blob/master/docs/why-fuzz.md)
- [Chromium's Getting Started](https://chromium.googlesource.com/chromium/src/+/main/testing/libfuzzer/getting_started.md)

## Building the fuzz tests

The fuzz tests are built in "unit test" mode by default on macOS and Linux.
MSVC (Windows) is not supported by the fuzztest library, and the tests are not
build there.

There are three ways of building the tests:
 - unit test mode - the default
 ```
-DFUZZTEST_FUZZING_MODE=OFF
 ```
 - fuzz test mode - enables the built-in fuzzing engine ([Centipede](https://github.com/google/fuzztest/blob/main/centipede/README.md))
 ```
-DFUZZTEST_FUZZING_MODE=ON -DCB_ADDRESSSANITIZER=ON
 ```
 - fuzz test with libFuzzer mode - uses the experimental support for libFuzzer
 ```
-DFUZZTEST_COMPATIBILITY_MODE=libfuzzer -DCB_ADDRESSSANITIZER=ON
 ```

Both the Centipede and libFuzzer modes require a sanitized build and support
Clang only (not GCC).

### Centipede or libFuzzer

The Centipede fuzzing engine is the built-in one and is officially supported.
Therefore, we should use it unless there is a specific use case which it does
not support.

Note that currently, libFuzzer is ranking better on FuzzBench, but Centipede is
continuously being improved.

See also:
 - [FuzzBench from 2024-08-10](https://www.fuzzbench.com/reports/2024-08-10-test/index.html)

### macOS only

Apple does not distribute libFuzzer in it's AppleClang, so building with
libFuzzer will fail by default. However, you can just symlink the library
from the homebrew LLVM into the AppleClang paths.

```
brew install llvm@17
sudo ln -s /opt/homebrew/opt/llvm@17//lib/clang/17/lib/darwin/libclang_rt.fuzzer_osx.a /Library/Developer/CommandLineTools/usr/lib/clang/15.0.0/lib/darwin/libclang_rt.fuzzer_osx.a
```

## Writing fuzz tests

Fuzz tests which are built using the google/fuzztest library should:
 - be added to CTest with a prefix "fuzztest."
 - use the --fromenv=fuzz_for flag, to allow for the fuzz duration to be
 specified as an environment variable (example: `FLAGS_fuzz_for=30s`)

This makes it possible to run all fuzz tests with a fuzzing duration specified
at runtime:
```sh
FLAGS_fuzz_for=30s ctest -R ^fuzztest
```

Note: The CTest targets are only registered when `FUZZTEST_FUZZING_MODE` is
enabled.

## Running the fuzz tests

On supported platforms (not Windows), the fuzz tests are compiled and linked
into the unit tests executable. They are also registered with GTest and CTest.
When running the test executables without any additional flags, the fuzz tests
are run for 1 second with random inputs. This ensures some minimal coverage of
the added fuzz tests in CV.

The fuzz test suites should all end in FuzzTest to be able to distinguish them
from unit tests.

To run the EP-Engine fuzz tests tests for 1 second with random inputs:
```
ctest -R ep-engine_ep_unit_tests.*FuzzTest
```

To run a specific fuzz test in fuzzing mode:
```
ep-engine_ep_unit_tests --fuzz=TestCase --fuzz-for=10s
```

To run all fuzz tests in fuzzing mode:
```sh
FLAGS_fuzz_for=10s ctest -R ^fuzztest
```

See also:
- [fuzztest flags](https://github.com/google/fuzztest/blob/main/doc/flags-reference.md)
