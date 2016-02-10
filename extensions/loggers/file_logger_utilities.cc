/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <platform/dirutils.h>
#include "file_logger_utilities.h"

unsigned long find_first_logfile_id(const std::string& basename) {
    using namespace CouchbaseDirectoryUtilities;

    unsigned long id = 0;

    auto files = findFilesWithPrefix(basename);
    for (auto& file : files) {
        // the format of the name should be:
        // fnm.number.txt
        auto index = file.rfind(".txt");
        if (index == std::string::npos) {
            continue;
        }

        file.resize(index);
        index = file.rfind('.');
        if (index != std::string::npos) {
            try {
                unsigned long value = std::stoul(file.substr(index + 1));
                if (value > id) {
                    id = value + 1;
                }
            } catch (...) {
                // Ignore
            }
        }
    }

    return id;
}
