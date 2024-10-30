/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <memcached/vbucket.h>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

/**
 * The VBucketEncryptionKeysManager class is responsible for keeping
 * track of the encryption key used for the various vbuckets.
 * It keeps track of two keys; "current" and "next"
 *
 * The current key is the one currently used by the vbucket, and the
 * (optional) next key is the one currently used by "compaction"
 * which may be the same as the current key and it may or may not
 * be promoted to the current key (depending if compaction completes
 * or not)
 */
class VBucketEncryptionKeysManager {
public:
    VBucketEncryptionKeysManager() = default;

    /// Set the key in used for the active vbucket (or nullopt) if no
    /// key is to be stored
    void setCurrentKey(Vbid vb, std::string key);
    void removeCurrentKey(Vbid vb);

    /// Set the key in used by compaction for the provided vbucket (or nullopt)
    /// if no key is to be stored
    void setNextKey(Vbid vb, std::string key);
    void removeNextKey(Vbid vb);

    /// Promote the key stored for the compaction entry to the current
    /// active
    void promoteNextKey(Vbid vb);

    /// Get all (unique) keys in use
    std::unordered_set<std::string> getKeys() const;

protected:
    using EncryptionKeyPair =
            std::pair<std::optional<std::string>, std::optional<std::string>>;
    folly::Synchronized<std::unordered_map<Vbid, EncryptionKeyPair>, std::mutex>
            encryptionKeyPairs;
};
