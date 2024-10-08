/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "oauthbearer/oauthbearer.h"

#include <cbsasl/client.h>
#include <cbsasl/plain/plain.h>
#include <cbsasl/scram-sha/scram-sha.h>
#include <memory>

namespace cb::sasl::client {
ClientContext::ClientContext(
        GetUsernameCallback user_cb,
        GetPasswordCallback password_cb,
        const std::string& mechanisms,
        const std::function<std::string()>& generate_nonce_function,
        std::function<void(char, const std::string&)> scram_property_listener) {
    switch (selectMechanism(mechanisms)) {
    case Mechanism::OAUTHBEARER:
        backend = std::make_unique<mechanism::oauthbearer::ClientBackend>(
                user_cb, password_cb, *this);
        break;
    case Mechanism::SCRAM_SHA512:
        backend = std::make_unique<mechanism::scram::Sha512ClientBackend>(
                user_cb,
                password_cb,
                *this,
                generate_nonce_function,
                std::move(scram_property_listener));
        break;
    case Mechanism::SCRAM_SHA256:
        backend = std::make_unique<mechanism::scram::Sha256ClientBackend>(
                user_cb,
                password_cb,
                *this,
                generate_nonce_function,
                std::move(scram_property_listener));
        break;
    case Mechanism::SCRAM_SHA1:
        backend = std::make_unique<mechanism::scram::Sha1ClientBackend>(
                user_cb,
                password_cb,
                *this,
                generate_nonce_function,
                std::move(scram_property_listener));
        break;
    case Mechanism::PLAIN:
        backend = std::make_unique<mechanism::plain::ClientBackend>(
                user_cb, password_cb, *this);
    }

    if (!backend) {
        throw unknown_mechanism(
                "cb::sasl::client::ClientContext(): Failed to create "
                "mechanism");
    }
}

} // namespace cb::sasl::client
