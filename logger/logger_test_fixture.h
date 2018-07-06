/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "logger.h"

#include <platform/dirutils.h>

#include <boost/optional.hpp>
#include <gtest/gtest.h>

class SpdloggerTest : virtual public ::testing::Test {
protected:
/*
 * Unset a few environment variables which affect how the logger works.
 * unsetenv() is not supported on Windows.
 */
#ifndef WIN32

    static void SetUpTestCase() {
        unsetenv("CB_MAXIMIZE_LOGGER_CYCLE_SIZE");
        unsetenv("CB_MAXIMIZE_LOGGER_BUFFER_SIZE");
    }

#endif

    virtual void SetUp();
    virtual void TearDown();

    /**
     * Helper function - initializes a cb logger object using the given
     * parameters
     *
     * @param level the minimum logging level
     * @param cyclesize the size to use before switching file (default 20MB)
     */
    virtual void setUpLogger(const spdlog::level::level_enum level,
                             const size_t cyclesize = 20 * 1024);

    /**
     * Helper function - removes the files in the test working directory that
     * are prefixed with the given filename
     */
    void RemoveFiles();

    /**
     * Helper function - counts how many times a string appears in a file.
     *
     * @param file the name of the file
     * @param msg the message to search for
     * @return the number of times we found the message in the file
     */
    static int countInFile(const std::string& file, const std::string& msg);

    std::string getLogContents();

    std::vector<std::string> files;
    std::string filename{"spdlogger_test"};
    const std::string openingHook = "---------- Opening logfile: ";
    const std::string closingHook = "---------- Closing logfile";
};
