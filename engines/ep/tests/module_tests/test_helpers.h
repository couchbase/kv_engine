/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Helper functions / code for unit tests
 */

#pragma once

#include "diskdockey.h"
#include "ep_types.h"

#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <memcached/storeddockey.h>

#include <chrono>

// The way magma set its memory quota is to use 10% of the
// max_size per shard. Set this to allow for 3MB per shard assuming
// there are 4 shards.
// 3145728 * 4 / 0.1 = 125829120
static std::string magmaConfig =
        "max_size=125829120;"
        "magma_flusher_thread_percentage=50";

// When a test needs to do a rollback, we need to configure magma
// to generate a rollback point with each item batch, similar to what
// couchstore does.
static std::string magmaRollbackConfig =
        "magma_max_checkpoints=10;"
        "magma_checkpoint_interval=0;"
        "magma_min_checkpoint_interval=0;"
        "magma_sync_every_batch=true";

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
                                    cb::durability::Level::Majority,
                                    cb::durability::Timeout()});

queued_item makeAbortedItem(StoredDocKey key, const std::string& value);

/// Make a queued_item representing a commited (normal mutation).
queued_item makeCommittedItem(StoredDocKey key,
                              std::string value,
                              Vbid vbid = Vbid(0));

/// Make a queued_item representing a commited SyncWrite.
queued_item makeCommittedviaPrepareItem(StoredDocKey key, std::string value);

/// Make a queued_item representing a deletion.
queued_item makeDeletedItem(StoredDocKey key);

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
bool queueNewItem(VBucket& vbucket, DocKey key);

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
    explicit TimeTraveller(int by);

    ~TimeTraveller();

    int get() const {
        return by;
    }

private:
    // Amount of time travel.
    int by;
};

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

// Return a string suitable for a database directory path, based on the
// current running GoogleTest.
std::string dbnameFromCurrentGTestInfo();

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

/**
 * Removes the given path if it exists, NOP otherwise.
 *
 * @param path
 * @throws std::system_error If the removal fails for reason other than ENOENT
 */
void removePathIfExists(const std::string& path);

/**
 * Get the bucket type from the config string
 *
 * @param config Test config string
 * @return Bucket type
 */
std::string getBucketType(std::string_view config);

/**
 * Generate a bucket type + bucket backend config string from the test config
 * string
 *
 * e.g. "persistent_couchstore" -> "bucketType=persistent;backend=couchdb"
 * e.g. "persistent_magma" -> "bucketType=persistent;backend=magma"
 * e.g. "ephemeral" -> "bucketType=ephemeral"
 *
 * @param config Test config string
 * @return Bucket config string
 */
std::string generateBucketTypeConfig(std::string_view config);

/**
 * Generate a bucket backend config string from the test config string
 *
 * e.g. "persistent_couchstore" -> "backend=couchdb"
 * e.g. "persistent_magma" -> "backend=magma"
 * e.g. "ephemeral" -> throws
 *
 * @param config Test config string
 * @return Bucket config string
 * @throws invalid_argument if not a persistent bucket type
 */
std::string generateBackendConfig(std::string_view config);

/**
 * Generates a config string for NexusKVStore usage from the testConfig
 *
 * @param config config for the test
 * @return config string
 */
std::string generateNexusConfig(std::string_view testConfig);

/**
 * Sanitizes a config string parameter that we use to config GTest parameterized
 * test suites. We need to sanitize the parameter string as GTest does not allow
 * ';' characters in parameters but we use them in configuration.json config so
 * our parameters use ':' as a placeholder. We also replace "couchstore" with
 * "couchdb".
 *
 * @param config for the test
 * @return config string
 */
std::string sanitizeTestParamConfigString(std::string_view config);