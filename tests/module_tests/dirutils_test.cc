/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "couch-kvstore/dirutils.hh"
#include <sys/stat.h>
#include <cerrno>
#include <gtest/gtest.h>

using namespace CouchKVStoreDirectoryUtilities;
using namespace std;

#include <iostream>

class DirnameTest : public ::testing::Test
{
};

TEST_F(DirnameTest, HandleEmptyString)
{
    EXPECT_EQ(".", dirname(""));
}

TEST_F(DirnameTest, HandleNoDirectorySeparator)
{
    EXPECT_EQ(".", dirname("foo"));
}

TEST_F(DirnameTest, HandleRootDirectory)
{
    EXPECT_EQ("\\", dirname("\\foo"));
    EXPECT_EQ("/", dirname("/foo"));
}

TEST_F(DirnameTest, HandleSingleDirectory)
{
    EXPECT_EQ("foo", dirname("foo\\bar"));
    EXPECT_EQ("foo", dirname("foo/bar"));
}

TEST_F(DirnameTest, HandleRootedSingleDirectory)
{
    EXPECT_EQ("\\foo", dirname("\\foo\\bar"));
    EXPECT_EQ("/foo", dirname("/foo/bar"));
}

TEST_F(DirnameTest, HandleTwolevelDirectory)
{
    EXPECT_EQ("foo\\bar", dirname("foo\\bar\\foobar"));
    EXPECT_EQ("foo/bar", dirname("foo/bar/foobar"));
}

TEST_F(DirnameTest, HandleRootedTwolevelDirectory)
{
    EXPECT_EQ("\\foo\\bar", dirname("\\foo\\bar\\foobar"));
    EXPECT_EQ("/foo/bar", dirname("/foo/bar/foobar"));
}

class BasenameTest : public ::testing::Test
{
};

TEST_F(BasenameTest, HandleEmptyString)
{
    EXPECT_EQ("", basename(""));
}

TEST_F(BasenameTest, HandleNoDirectory)
{
    EXPECT_EQ("foo", basename("foo"));
}

TEST_F(BasenameTest, HandleRootDirectory)
{
    EXPECT_EQ("foo", basename("\\foo"));
    EXPECT_EQ("foo", basename("/foo"));
}

TEST_F(BasenameTest, HandleSingleDirectory)
{
    EXPECT_EQ("bar", basename("foo\\bar"));
    EXPECT_EQ("bar", basename("foo/bar"));
}

TEST_F(BasenameTest, HandleRootedSingleDirectory)
{
    EXPECT_EQ("bar", basename("\\foo\\bar"));
    EXPECT_EQ("bar", basename("/foo/bar"));
}

TEST_F(BasenameTest, HandleTwoLevelDirectory)
{
    EXPECT_EQ("foobar", basename("foo\\bar\\foobar"));
    EXPECT_EQ("foobar", basename("foo/bar/foobar"));
}

TEST_F(BasenameTest, HandleRootedTwoLevelDirectory)
{
    EXPECT_EQ("foobar", basename("\\foo\\bar\\foobar"));
    EXPECT_EQ("foobar", basename("/foo/bar/foobar"));
}

class DiskMatchingTest : public ::testing::Test
{
public:
    DiskMatchingTest() {
        files.push_back("my-dirutil-test/a.0");
        files.push_back("my-dirutil-test/a.1");
        files.push_back("my-dirutil-test/a.2");
        files.push_back("my-dirutil-test/a.3");
        files.push_back("my-dirutil-test/b.0");
        files.push_back("my-dirutil-test/b.1");
        files.push_back("my-dirutil-test/c.0");
        files.push_back("my-dirutil-test/c.1");
        files.push_back("my-dirutil-test/0.couch");
        files.push_back("my-dirutil-test/0.couch.0");
        files.push_back("my-dirutil-test/0.couch.2");
        files.push_back("my-dirutil-test/3.couch.compact");
        files.push_back("my-dirutil-test/1.couch");
        files.push_back("my-dirutil-test/2.couch");
        files.push_back("my-dirutil-test/3.couch");
        files.push_back("my-dirutil-test/4.couch");
        files.push_back("my-dirutil-test/5.couch");
        files.push_back("my-dirutil-test/w1");
        files.push_back("my-dirutil-test/w2");
    }

    virtual void SetUp(void) {
        if (mkdir("my-dirutil-test", S_IRWXU) == -1) {
            ASSERT_EQ(EEXIST, errno);
        }

        vector<string>::iterator ii;
        for (ii = files.begin(); ii != files.end(); ++ii) {
            ASSERT_TRUE(touch(*ii));
        }
    }

    virtual void TearDown(void) {

        vector<string>::iterator ii;
        for (ii = files.begin(); ii != files.end(); ++ii) {
            ASSERT_EQ(0, remove((*ii).c_str()));
        }

        ASSERT_EQ(0, remove("my-dirutil-test"));
    }

protected:
    bool inList(const vector<string> &list, const string &name) {
        vector<string>::const_iterator ii;
        for (ii = list.begin(); ii != list.end(); ++ii) {
            if (*ii == name) {
                return true;
            }
        }

        return false;
    }

    bool touch(const string &name) {
        FILE *fp = fopen(name.c_str(), "w");
        if (fp != NULL) {
            fclose(fp);
            return true;
        }
        return false;
    }

    vector<string> files;
};

TEST_F(DiskMatchingTest, NonExistingDirectory)
{
    vector<string> f1 = findFilesWithPrefix("my-nonexisting", "dir");
    EXPECT_EQ(0, f1.size());

    vector<string> f2 = findFilesWithPrefix("my-nonexisting/dir");
    EXPECT_EQ(0, f2.size());
}

TEST_F(DiskMatchingTest, findAllFiles)
{
    vector<string> f1 = findFilesWithPrefix("my-dirutil-test", "");
    EXPECT_LE(files.size(), f1.size());
    vector<string>::const_iterator ii;

    for (ii = files.begin(); ii != files.end(); ++ii) {
        EXPECT_TRUE(inList(f1, *ii));
    }

    vector<string> f2 = findFilesWithPrefix("my-dirutil-test/");
    EXPECT_LE(files.size(), f2.size());
    for (ii = files.begin(); ii != files.end(); ++ii) {
        EXPECT_TRUE(inList(f2, *ii));
    }
}

TEST_F(DiskMatchingTest, findA0)
{
    vector<string> f1 = findFilesWithPrefix("my-dirutil-test", "a.0");
    EXPECT_EQ(1, f1.size());

    vector<string> f2 = findFilesWithPrefix("my-dirutil-test/a.0");
    EXPECT_EQ(1, f2.size());
}

TEST_F(DiskMatchingTest, findAllA)
{
    vector<string> f1 = findFilesWithPrefix("my-dirutil-test", "a");
    EXPECT_EQ(4, f1.size());

    EXPECT_TRUE(inList(f1, "my-dirutil-test/a.0"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/a.1"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/a.2"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/a.3"));

    vector<string> f2 = findFilesWithPrefix("my-dirutil-test/a");
    EXPECT_EQ(4, f2.size());

    EXPECT_TRUE(inList(f2, "my-dirutil-test/a.0"));
    EXPECT_TRUE(inList(f2, "my-dirutil-test/a.1"));
    EXPECT_TRUE(inList(f2, "my-dirutil-test/a.2"));
    EXPECT_TRUE(inList(f2, "my-dirutil-test/a.3"));
}

TEST_F(DiskMatchingTest, matchNoDirSubString)
{
    vector<string> f1 = findFilesContaining("", "");
    EXPECT_EQ(0, f1.size());
}

TEST_F(DiskMatchingTest, matchEmptySubString)
{
    vector<string> f1 = findFilesContaining("my-dirutil-test", "");
    EXPECT_LE(files.size(), f1.size());
}

TEST_F(DiskMatchingTest, matchSingleCharSubString)
{
    vector<string> f1 = findFilesContaining("my-dirutil-test", "w");
    EXPECT_EQ(2, f1.size());
    EXPECT_TRUE(inList(f1, "my-dirutil-test/w1"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/w2"));
}

TEST_F(DiskMatchingTest, matchLongerSubString)
{
    vector<string> f1 = findFilesContaining("my-dirutil-test", "couch");
    EXPECT_EQ(9, f1.size());

    EXPECT_TRUE(inList(f1, "my-dirutil-test/0.couch"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/0.couch.0"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/0.couch.2"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/3.couch.compact"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/1.couch"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/2.couch"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/3.couch"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/4.couch"));
    EXPECT_TRUE(inList(f1, "my-dirutil-test/5.couch"));
}

TEST_F(DiskMatchingTest, matchTailSubString)
{
    vector<string> f1 = findFilesContaining("my-dirutil-test", "compact");
    EXPECT_EQ(1, f1.size());
    EXPECT_TRUE(inList(f1, "my-dirutil-test/3.couch.compact"));
}


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
