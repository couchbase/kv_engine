/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "logger_test_common.h"

#include <platform/cbassert.h>
#include <platform/dirutils.h>

#include <cstring>
#include <cstdio>
#include <iostream>
#include <sstream>

#include <gtest/gtest.h>

#include "extensions/loggers/file_logger_utilities.h"

class LoggerTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        files = cb::io::findFilesWithPrefix("logger_test");

        if (!files.empty()) {
            remove_files(files);
        }

        /* Note: Ensure buffer is at least 4* larger than the expected message
         * length, otherwise the writer will be blocked waiting for the flusher
         * thread to timeout and write (as we haven't actually hit the 75%
         * watermark which would normally trigger an immediate flush).
         */
        ret = memcached_extensions_initialize("unit_test=true;"
                  "cyclesize=2048;buffersize=8192;"
                  "sleeptime=1;filename=logger_test", get_server_api);
        cb_assert(ret == EXTENSION_SUCCESS);

    }
    EXTENSION_ERROR_CODE ret;
    std::vector<std::string> files;
};


TEST_F(LoggerTest, MessageSizeBigWithNewLine) {
    char message[2048];
    for (auto i = 0; i < 2046; i++) {
        message[i] = 'x';
    }
    message[2046] = '\n';
    message[2047] = '\0';
    logger->log(EXTENSION_LOG_DETAIL, nullptr, message);
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix("logger_test");
    cb_assert(files.size() == 1);
    remove_files(files);
}


TEST_F(LoggerTest, MessageSizeBigWithNoNewLine) {
    char message[2048];
    for (auto i = 0; i < 2047; i++) {
        message[i] = 'x';
    }
    message[2047] = '\0';
    logger->log(EXTENSION_LOG_DETAIL, nullptr, message);
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix("logger_test");
    cb_assert(files.size() == 1);
    remove_files(files);
}


TEST_F(LoggerTest, MessageSizeSmallWithNewLine) {
    logger->log(EXTENSION_LOG_DETAIL, nullptr, "small_message\n");
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix("logger_test");
    cb_assert(files.size() == 1);
    remove_files(files);
}


TEST_F(LoggerTest, MessageSizeSmallWithNoNewLine) {
    logger->log(EXTENSION_LOG_DETAIL, nullptr, "small_message");
    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix("logger_test");
    cb_assert(files.size() == 1);
    remove_files(files);
}


TEST_F(LoggerTest, Rotate) {
    for (auto ii = 0; ii < 8192; ++ii) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                    "Hei hopp, dette er bare noe tull... Paa tide med %05u!!",
                    ii);
    }

    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix("logger_test");

    // The cyclesize isn't a hard limit. We don't truncate entries that
    // won't fit to move to the next file. We'll rather dump the entire
    // buffer to the file. This means that each file may in theory be
    // up to a buffersize bigger than the cyclesize.. It is a bit hard
    // determine the exact number of files we should get here.. Testing
    // on MacOSX I'm logging 97 bytes in my timezone (summertime), but
    // on Windows it turned out to be a much longer timezone name etc..
    // I'm assuming that we should end up with 90+ files..
    cb_assert(files.size() >= 90);
    remove_files(files);
}

static bool my_fgets(char *buffer, size_t buffsize, FILE *fp) {
    if (fgets(buffer, (int)buffsize, fp) != NULL) {
        char *end = strchr(buffer, '\n');
        cb_assert(end);
        *end = '\0';
        if (*(--end) == '\r') {
            *end = '\0';
        }
    return true;
    } else {
        return false;
    }
}

static std::string create_filename(const std::string &prefix,
                                   const std::string &postfix) {
    std::stringstream ss;
    ss << prefix << "." << time(NULL) << "." << postfix;
    return ss.str();
}

// There are too many spurious test failures with this test.
TEST_F(LoggerTest, DISABLED_Dedupe) {
    EXTENSION_ERROR_CODE ret;
    int ii;
    std::string filename = create_filename("logger_test", "dedupe");

    std::vector<std::string> files;
    files = cb::io::findFilesWithPrefix(filename);
    if (!files.empty()) {
        remove_files(files);
        files = cb::io::findFilesWithPrefix(filename);
        if (!files.empty()) {
            std::cerr << "ERROR: Failed to remove all files: " << std::endl;
            for (auto f : files) {
                std::cerr << "\t" << f << std::endl;
            }
            exit(EXIT_FAILURE);
        }
    }

    std::string config("unit_test=true;"
                       "cyclesize=1024;"
                       "buffersize=1024;"
                       "sleeptime=1;"
                       "filename=");
    config.append(filename);

    ret = memcached_extensions_initialize(config.c_str(),
                                          get_server_api);
    cb_assert(ret == EXTENSION_SUCCESS);

    for (ii = 0; ii < 1024; ++ii) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                    "Hei hopp, dette er bare noe tull... Paa tide med kaffe");
    }

    logger->shutdown(false);

    files = cb::io::findFilesWithPrefix(filename);
    if (files.size() != 1) {
        std::cerr << "Expected one file, found " << files.size() << ":"
                  << std::endl;
        for (auto f : files) {
            std::cerr << "\t" << f << std::endl;
        }
        exit(EXIT_FAILURE);
    }

    FILE *fp = fopen(files[0].c_str(), "r");
    if (fp == NULL) {
        std::cerr << "Failed to open " << files[0] << ": "
                  << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }
    char buffer[1024];

    while (my_fgets(buffer, sizeof(buffer), fp)) {
        /* EMPTY */
    }

    if (strstr(buffer, "repeated 1023 times") == NULL) {
        std::cerr << "Incorrect deduplication message:" << std::endl
                  << "Expected to find [repeated 1023 times]"
                  << std::endl
                  << "Found [" << buffer << "]"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    fclose(fp);
    remove_files(files);
}

class LoggerUtilitiesTest : public ::testing::Test {
public:

    LoggerUtilitiesTest()
        : prefix("logger-util") { }

    void SetUp() {
        cleanup();
    }

    void TearDown() {
        cleanup();
    }

protected:
    void createFile(unsigned long id, const std::string& sep = ".",
                    const std::string& suffix = ".txt") {
        std::string fname(prefix);
        fname.append(sep);
        fname.append(std::to_string(id));
        fname.append(suffix);
        FILE* fp = fopen(fname.c_str(), "a");
        ASSERT_NE(nullptr, fp);
        fclose(fp);
    }

    void cleanup() {
        auto files = cb::io::findFilesWithPrefix(prefix);
        for (auto& file : files) {
            cb::io::rmrf(file);
        }
    }

    std::string prefix;
};

TEST_F(LoggerUtilitiesTest, NoFiles) {
    EXPECT_EQ(0, find_first_logfile_id(prefix));
}

TEST_F(LoggerUtilitiesTest, OneFile) {
    createFile(1);
    EXPECT_EQ(2, find_first_logfile_id(prefix));
}

TEST_F(LoggerUtilitiesTest, MultipleFile) {
    createFile(1);
    createFile(100);
    createFile(5);
    createFile(1000);
    createFile(2);
    EXPECT_EQ(1001, find_first_logfile_id(prefix));
}

TEST_F(LoggerUtilitiesTest, TestInvalidExtension) {
    createFile(1, ".", "");
    EXPECT_EQ(0, find_first_logfile_id(prefix));
}

TEST_F(LoggerUtilitiesTest, TestInvalidSeparator) {
    createFile(1, "-", ".txt");
    EXPECT_EQ(0, find_first_logfile_id(prefix));
}
