/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "cluster_config.h"

#include <subdoc/operations.h>

#include <cstdlib>
#include <stdexcept>

void ClusterConfiguration::setConfiguration(std::string_view buffer, int rev) {
    std::lock_guard<std::mutex> guard(mutex);
    revision = rev;
    config = std::make_shared<std::string>(buffer.begin(), buffer.end());
}

void ClusterConfiguration::setConfiguration(std::string_view buffer) {
    int rev = getRevisionNumber(buffer);
    if (rev == NoConfiguration) {
        throw std::invalid_argument(
                "ClusterConfiguration::setConfiguration: Failed determine map "
                "revision");
    }

    std::lock_guard<std::mutex> guard(mutex);
    revision = rev;
    config = std::make_shared<std::string>(buffer.begin(), buffer.end());
}

int ClusterConfiguration::getRevisionNumber(std::string_view buffer) {
    Subdoc::Operation operation;
    Subdoc::Result result;
    operation.set_code(Subdoc::Command::GET);
    operation.set_doc(buffer.data(), buffer.size());
    operation.set_result_buf(&result);

    if (operation.op_exec("rev") != Subdoc::Error::SUCCESS) {
        return NoConfiguration;
    }

    auto loc = result.matchloc();
    std::string value{loc.at, loc.length};

    try {
        std::size_t count;
        auto ret = std::stoi(value, &count);
        if (count == loc.length) {
            return ret;
        }
    } catch (const std::exception&) {
    }

    return NoConfiguration;
}

void ClusterConfiguration::reset() {
    std::lock_guard<std::mutex> guard(mutex);
    revision = NoConfiguration;
    config = std::make_shared<std::string>();
}
