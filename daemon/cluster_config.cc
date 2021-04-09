/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
