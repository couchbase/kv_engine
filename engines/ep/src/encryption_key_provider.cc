/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "encryption_key_provider.h"

#include <bucket_logger.h>

cb::crypto::SharedEncryptionKey EncryptionKeyProvider::lookup(
        std::string_view id) const {
    if (id.empty()) {
        return keyStore.lock()->getActiveKey();
    }
    return keyStore.lock()->lookup(id);
}

void EncryptionKeyProvider::setKeys(cb::crypto::KeyStore keys) {
    EP_LOG_INFO_RAW("Setting new encryption keys");
    keyStore = keys;
    listeners.withLock([&keys](auto& functions) {
        if (!functions.empty()) {
            EP_LOG_INFO_RAW("Notify kvstore of new encryption keys");
            for (const auto& function : functions) {
                function(keys);
            }
            EP_LOG_INFO_RAW("Done notifying kvstore of new encryption keys");
        }
    });
}

void EncryptionKeyProvider::addListener(
        std::function<void(const cb::crypto::KeyStore&)> listener) {
    listeners.lock()->emplace_back(std::move(listener));
}
