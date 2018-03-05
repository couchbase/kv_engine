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

/*
 * Helper functions / code for unit tests
 */

#pragma once

#include "config.h"

#include "item.h"
#include "programs/engine_testapp/mock_server.h"

#include <chrono>

/// Creates an item with the given vbucket id, key and value.
Item make_item(
        uint16_t vbid,
        const StoredDocKey& key,
        const std::string& value,
        uint32_t exptime = 0,
        protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON);

std::unique_ptr<Item> makeCompressibleItem(uint16_t vbid,
                                           const StoredDocKey& key,
                                           const std::string& value,
                                           protocol_binary_datatype_t datatype,
                                           bool shouldCompress,
                                           bool makeXattrBody = false);

/**
 * Create a StoredDocKey object from a std::string.
 * By default places the key in the default namespace,
 * DocNamespace::DefaultCollection.
 */
inline StoredDocKey makeStoredDocKey(
        const std::string& string,
        DocNamespace ns = DocNamespace::DefaultCollection) {
    return StoredDocKey(string, ns);
}

/**
 * Create an XATTR document using the supplied string as the body
 * @returns string containing the new value
 */
std::string createXattrValue(const std::string& body,
                             bool withSystemKey = true,
                             bool makeItSnappy = false);

/**
 * Class which moves time forward when created by the given amount, and upon
 * destruction returns time to where it was.
 *
 * Allows tests to manipulate server time, but need to ensure any adjustments
 * are restored so as to not affect other later tests.
 */
class TimeTraveller {
public:
    TimeTraveller(int by)
        : by(by) {
        mock_time_travel(by);
    }

    ~TimeTraveller() {
        // restore original timeline.
        mock_time_travel(-by);
    }

private:
    // Amount of time travel.
    int by;
};

/**
 * Function to do an exponentially increasing, but max bounded, sleep.
 * To do exponentially increasing sleep, must be called first with the starting
 * sleep time and subsequently with the sleep time returned in the previous call
 *
 * @param uSeconds Desired sleep time in micro seconds
 *
 * @return indicates the next sleep time (doubled from the current value)
 */
std::chrono::microseconds decayingSleep(std::chrono::microseconds uSeconds);
