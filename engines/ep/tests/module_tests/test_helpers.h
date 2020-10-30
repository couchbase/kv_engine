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

#include "diskdockey.h"
#include "ep_types.h"
#include "storeddockey.h"

#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>

#include <chrono>

class VBucket;

/// Creates an item with the given vbucket id, key and value.
Item make_item(
        Vbid vbid,
        const DocKey& key,
        const std::string& value,
        uint32_t exptime = 0,
        protocol_binary_datatype_t datatype = PROTOCOL_BINARY_DATATYPE_JSON);

/// Make a queued_item representing a pending SyncWrite.
queued_item makePendingItem(StoredDocKey key,
                            const std::string& value,
                            cb::durability::Requirements reqs = {
                                    cb::durability::Level::Majority, {}});

/// Make a queued_item representing a commited (normal mutation).
queued_item makeCommittedItem(StoredDocKey key, std::string value);

/// Make a queued_item representing a commited SyncWrite.
queued_item makeCommittedviaPrepareItem(StoredDocKey key, std::string value);

std::unique_ptr<Item> makeCompressibleItem(Vbid vbid,
                                           const DocKey& key,
                                           const std::string& value,
                                           protocol_binary_datatype_t datatype,
                                           bool shouldCompress,
                                           bool makeXattrBody = false);

/**
 * Create a StoredDocKey object from a std::string.
 * By default places the key in the default namespace,
 * CollectionID::Default.
 */
StoredDocKey makeStoredDocKey(const std::string& string,
                              CollectionID ns = CollectionID::Default);

/**
 * Create a DiskDocKey object from a std::string.
 * By default places the key in the default namespace,
 * CollectionID::Default.
 */
DiskDocKey makeDiskDocKey(const std::string& string,
                          bool prepare = false,
                          CollectionID ns = CollectionID::Default);

// Creates a new item with the given key and queues it into the given VBucket.
// manager.
bool queueNewItem(VBucket& vbucket, const std::string& key);

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
    TimeTraveller(int by);

    ~TimeTraveller();

    int get() const {
        return by;
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

/**
 * Rewrite the vbstate of the vbucket/revision
 * i.e. the file @ dbDir/<vbid>.couch.<revision>
 * This method makes the vbstate appear to be from the past so we can test
 * some upgrade scenarios.
 */
void rewriteCouchstoreVBState(Vbid vbucket,
                              const std::string& dbDir,
                              int revision,
                              bool namespacesSupported = true);

/**
 * Modify the vbstate of the vbucket/revision
 * i.e. the file @ dbDir/<vbid>.couch.<revision>
 * @param vBucket Vbucket ID
 * @param dbDir Directory the couchstore file resides
 * @param revision of the file to modify
 * @param modifyFn Callback which is invoked on the current vbState JSON value.
 *        The updated JSON object is written back to disk.
 *
 * This method allows the '_local/vbstate' on-disk document to be arbitrarily
 * modified, for example for upgrade or error-injection tests.
 */
void modifyCouchstoreVBState(
        Vbid vbucket,
        const std::string& dbDir,
        int revision,
        std::function<void(nlohmann::json& vbState)> modifyFn);
