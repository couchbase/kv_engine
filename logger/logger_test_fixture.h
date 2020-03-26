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
#pragma once

// Spdlog includes it's own portability code (details/os.h) to allow it to run
// on multiple platforms, notably it includes <io.h>. This leads to ambiguous
// symbol declarations when we come to include folly's GTest.h as it includes
// it's own portability header Unistd.h. Include folly's portability code before
// anything else to fix this.
#include <folly/portability/GTest.h>

#include "logger.h"
#include "logger_config.h"

#include <platform/dirutils.h>

#include <optional>

class SpdloggerTest : virtual public ::testing::Test {
protected:
    SpdloggerTest();

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

    void SetUp() override;
    void TearDown() override;

    /**
     * Helper function - initializes a cb logger object using the 'config'
     * member variable.
     */
    virtual void setUpLogger();

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

    cb::logger::Config config;

    const std::string openingHook = "---------- Opening logfile: ";
    const std::string closingHook = "---------- Closing logfile";
};
