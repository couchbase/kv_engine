/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "couch-kvstore/dirutils.hh"
#include <sys/stat.h>
#include <cerrno>

#undef NDEBUG
#include <cassert>


using namespace CouchKVStoreDirectoryUtilities;
using namespace std;

#include <iostream>

static void test_dirname(void) {
    assert(dirname("foo") == ".");
    assert(dirname("\\foo") == "\\");
    assert(dirname("foo\\bar") == "foo");
    assert(dirname("\\foo\\bar") == "\\foo");
    assert(dirname("foo\\bar\\foobar") == "foo\\bar");
    assert(dirname("\\foo\\bar\\foobar") == "\\foo\\bar");
    assert(dirname("/foo") == "/");
    assert(dirname("foo/bar") == "foo");
    assert(dirname("/foo/bar") == "/foo");
    assert(dirname("foo/bar/foobar") == "foo/bar");
    assert(dirname("/foo/bar/foobar") == "/foo/bar");

}

static void test_basename(void) {
    assert(basename("foo") == "foo");
    assert(basename("\\foo") == "foo");
    assert(basename("foo\\bar") == "bar");
    assert(basename("\\foo\\bar") == "bar");
    assert(basename("foo\\bar\\foobar") == "foobar");
    assert(basename("\\foo\\bar\\foobar") == "foobar");
    assert(basename("/foo") == "foo");
    assert(basename("foo/bar") == "bar");
    assert(basename("/foo/bar") == "bar");
    assert(basename("foo/bar/foobar") == "foobar");
    assert(basename("/foo/bar/foobar") == "foobar");
}

static int mymkdir(const char *path) {
    int ret = 0;
    if (mkdir(path, S_IRWXU) == -1) {
        if (errno != EEXIST) {
            ret = -1;
        }
    }

    return ret;
}

static int touch(const char *name) {
    FILE *fp = fopen(name, "w");
    if (fp != NULL) {
        fclose(fp);
        return 0;
    }
    return -1;
}

void assertInList(const vector<string> &files, const string &nm)
{
    for (size_t idx = 0; idx < files.size(); ++idx) {
        if (files[idx] == nm) {
            return;
        }
    }
    assert(false);
}

static void test_findFilesWithPrefix(void) {
    assert(mymkdir("my-dirutil-test") == 0);
    const char *filenames[] = {
        "my-dirutil-test/a.0",
        "my-dirutil-test/a.1",
        "my-dirutil-test/a.2",
        "my-dirutil-test/a.3",
        "my-dirutil-test/b.0",
        "my-dirutil-test/b.1",
        "my-dirutil-test/c.0",
        "my-dirutil-test/c.1",
        NULL
    };

    for (int idx = 0; filenames[idx] != NULL; ++idx) {
        assert(touch(filenames[idx]) == 0);
    }

    vector<string> files = findFilesWithPrefix("my-dirutil-test", "a.0");
    assert(files.size() == 1);
    assert(files[0] == "my-dirutil-test/a.0");

    files = findFilesWithPrefix("my-dirutil-test", "a.");
    assert(files.size() == 4);
    assertInList(files, "my-dirutil-test/a.0");
    assertInList(files, "my-dirutil-test/a.1");
    assertInList(files, "my-dirutil-test/a.2");
    assertInList(files, "my-dirutil-test/a.3");

    files = findFilesWithPrefix("my-dirutil-test", "");
    assert(files.size() >= 8);
    assertInList(files, "my-dirutil-test/a.0");
    assertInList(files, "my-dirutil-test/a.1");
    assertInList(files, "my-dirutil-test/a.2");
    assertInList(files, "my-dirutil-test/a.3");
    assertInList(files, "my-dirutil-test/b.0");
    assertInList(files, "my-dirutil-test/b.1");
    assertInList(files, "my-dirutil-test/c.0");
    assertInList(files, "my-dirutil-test/c.1");

    files = findFilesWithPrefix("my-nonexisting/dir", "foo");
    assert(files.size() == 0);

    files = findFilesWithPrefix("my-nonexisting/dir", "");
    assert(files.size() == 0);

    // Re-run the test with the other method to grab the files
    files = findFilesWithPrefix("my-dirutil-test/a.0");
    assert(files.size() == 1);
    assert(files[0] == "my-dirutil-test/a.0");

    files = findFilesWithPrefix("my-dirutil-test/a.");
    assert(files.size() == 4);
    assertInList(files, "my-dirutil-test/a.0");
    assertInList(files, "my-dirutil-test/a.1");
    assertInList(files, "my-dirutil-test/a.2");
    assertInList(files, "my-dirutil-test/a.3");

    files = findFilesWithPrefix("my-dirutil-test/");
    assert(files.size() >= 8);
    assertInList(files, "my-dirutil-test/a.0");
    assertInList(files, "my-dirutil-test/a.1");
    assertInList(files, "my-dirutil-test/a.2");
    assertInList(files, "my-dirutil-test/a.3");
    assertInList(files, "my-dirutil-test/b.0");
    assertInList(files, "my-dirutil-test/b.1");
    assertInList(files, "my-dirutil-test/c.0");
    assertInList(files, "my-dirutil-test/c.1");

    files = findFilesWithPrefix("my-nonexisting/dir/foo");
    assert(files.size() == 0);

    files = findFilesWithPrefix("my-nonexisting/dir", "");
    assert(files.size() == 0);

    for (int idx = 0; filenames[idx] != NULL; ++idx) {
        assert(remove(filenames[idx]) == 0);
    }

    assert(remove("my-dirutil-test") == 0);
}

int main(void)
{
    test_dirname();
    test_basename();
    test_findFilesWithPrefix();
    exit(EXIT_SUCCESS);
}
