/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vbucket_encryption_keys_manager.h"

#include <bucket_logger.h>

void VBucketEncryptionKeysManager::setCurrentKey(Vbid vb, std::string key) {
    encryptionKeyPairs.withLock(
            [&vb, &key](auto& map) { map[vb].first = std::move(key); });
}

void VBucketEncryptionKeysManager::removeCurrentKey(Vbid vb) {
    encryptionKeyPairs.withLock(
            [&vb](auto& map) { map[vb].first = std::nullopt; });
}

void VBucketEncryptionKeysManager::setNextKey(Vbid vb, std::string key) {
    encryptionKeyPairs.withLock(
            [&vb, &key](auto& map) { map[vb].second = std::move(key); });
}

void VBucketEncryptionKeysManager::removeNextKey(Vbid vb) {
    encryptionKeyPairs.withLock(
            [&vb](auto& map) { map[vb].second = std::nullopt; });
}

void VBucketEncryptionKeysManager::promoteNextKey(Vbid vb) {
    encryptionKeyPairs.withLock([vb](auto& map) {
        map[vb].first = std::move(map[vb].second);
        map[vb].second = std::nullopt;
    });
}

std::unordered_set<std::string> VBucketEncryptionKeysManager::getKeys() const {
    return encryptionKeyPairs.withLock([](const auto& map) {
        std::unordered_set<std::string> keys;
        for (const auto& [vb, pair] : map) {
            if (pair.first.has_value()) {
                keys.insert(pair.first.value());
            }
            if (pair.second.has_value()) {
                keys.insert(pair.second.value());
            }
        }
        return keys;
    });
}
