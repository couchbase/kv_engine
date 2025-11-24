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

#include <cbcrypto/key_store.h>
#include <folly/Synchronized.h>
#include <functional>

class EncryptionKeyProvider {
public:
    /**
     * Look up the actual key for the provided identifier (empty == the
     * default key to use for encryption). In the situation where encryption
     * is turned off the method will return an empty shared_ptr.
     *
     * @param id the key to look up (empty == current active key)
     * @return The requested key
     */
    [[nodiscard]] cb::crypto::SharedKeyDerivationKey lookup(
            std::string_view id) const;

    /**
     * Set all keys in the keystore to the provided value and notify all
     * registered listeners
     *
     * @param keys The new keystore to serve key requests from
     */
    void setKeys(cb::crypto::KeyStore keys);

    /**
     * Register the provided function to be called whenever the keystore change
     * @param listener The function to call
     */
    void addListener(std::function<void(const cb::crypto::KeyStore&)> listener);

protected:
    /// The actual key store to look up keys from
    folly::Synchronized<cb::crypto::KeyStore, std::mutex> keyStore;

    /// Listener functions called every time the list of encryption keys change
    folly::Synchronized<
            std::vector<std::function<void(const cb::crypto::KeyStore&)>>,
            std::mutex>
            listeners;
};
