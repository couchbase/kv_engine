This directory contains the Couchbase nightly test (CBNT) test config file.
This is a performance testing infrastructure used as part of commit validation
for both the memcached and ep-engine projects.

The couchbase-based test harnesses require this yaml config file to know which
tests to run.

The config file follows the following format:

```
- test: testname1
  command: "command to run this test"
  output:
    - "outputfile1.xml"
    - "outputfile2.xml"
- test: testname2
  command: "command to run this test"
  output: "outputfile.xml"
```

One really important thing to note about this config file is that all commands
should be relative to the root of the couchbase build directory
(i.e the directory which contains all of the projects; the result of a repo
sync) so that it can appropriately find all of the required files.
