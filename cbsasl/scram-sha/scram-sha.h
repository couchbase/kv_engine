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
#pragma once

/**
 * This file contains the interface to the SCRAM-SHA1, SCRAM-SHA256 and
 * SCRAM-512 support.
 *
 * SCRAM is defined in https://www.ietf.org/rfc/rfc5802.txt
 *
 * The current implementation does not support channel binding (so we
 * don't advertise the -PLUS)
 */

#include "../cbcrypto.h"
#include <cbsasl/client.h>
#include <cbsasl/server.h>
#include <cbsasl/user.h>
#include <array>
#include <iostream>
#include <vector>

namespace cb::sasl::mechanism::scram {

class ScramShaBackend {
protected:
    ScramShaBackend(const Mechanism& mech, const cb::crypto::Algorithm algo)
        : mechanism(mech), algorithm(algo) {
    }

    /**
     * Add a property to the message list according to
     * https://www.ietf.org/rfc/rfc5802.txt section 5.1
     *
     * The purpose of these conversion functions is that we want to
     * make sure that we enforce the right format on the various attributes
     * and that we detect illegal keys.
     *
     * @param out the destination stream
     * @param key the key to add
     * @param value the string representation of the attribute to add
     * @param more set to true if we should add a trailing comma (more data
     *             follows)
     */
    static void addAttribute(std::ostream& out,
                             char key,
                             const std::string& value,
                             bool more);

    /**
     * Add a property to the message list according to
     * https://www.ietf.org/rfc/rfc5802.txt section 5.1
     *
     * The purpose of these conversion functions is that we want to
     * make sure that we enforce the right format on the various attributes
     * and that we detect illegal keys.
     *
     * @param out the destination stream
     * @param key the key to add
     * @param value the integer value of the attribute to add
     * @param more set to true if we should add a trailing comma (more data
     *             follows)
     */
    static void addAttribute(std::ostream& out, char key, int value, bool more);

    std::string getServerSignature();
    std::string getClientSignature();

    virtual std::string getStoredKey() = 0;
    virtual std::string getServerKey() = 0;

    /**
     * Get the AUTH message (as specified in the RFC)
     */
    std::string getAuthMessage();

    std::string client_first_message;
    std::string client_first_message_bare;
    std::string client_final_message;
    std::string client_final_message_without_proof;
    std::string server_first_message;
    std::string server_final_message;

    std::string clientNonce;
    std::string serverNonce;
    std::string nonce;

    Mechanism mechanism;
    const cb::crypto::Algorithm algorithm;
};

class ServerBackend : public cb::sasl::server::MechanismBackend,
                      public ScramShaBackend {
public:
    ServerBackend(server::ServerContext& ctx,
                  Mechanism mechanism,
                  cb::crypto::Algorithm algo,
                  std::function<std::string()> generateNonceFunction);

    std::pair<Error, std::string_view> start(std::string_view input) override;

    std::pair<Error, std::string_view> step(std::string_view input) override;

    std::string getName() const final {
        return ::to_string(mechanism);
    }

protected:
    std::string getStoredKey() override {
        return user.getScramMetaData(algorithm).stored_key;
    }
    std::string getServerKey() override {
        return user.getScramMetaData(algorithm).server_key;
    }

    pwdb::User user;
};

class Sha512ServerBackend : public ServerBackend {
public:
    explicit Sha512ServerBackend(
            server::ServerContext& ctx,
            std::function<std::string()> generateNonceFunction = {})
        : ServerBackend(ctx,
                        Mechanism::SCRAM_SHA512,
                        cb::crypto::Algorithm::SHA512,
                        std::move(generateNonceFunction)) {
    }
};

class Sha256ServerBackend : public ServerBackend {
public:
    explicit Sha256ServerBackend(
            server::ServerContext& ctx,
            std::function<std::string()> generateNonceFunction = {})
        : ServerBackend(ctx,
                        Mechanism::SCRAM_SHA256,
                        cb::crypto::Algorithm::SHA256,
                        std::move(generateNonceFunction)) {
    }
};

class Sha1ServerBackend : public ServerBackend {
public:
    explicit Sha1ServerBackend(
            server::ServerContext& ctx,
            std::function<std::string()> generateNonceFunction = {})
        : ServerBackend(ctx,
                        Mechanism::SCRAM_SHA1,
                        cb::crypto::Algorithm::SHA1,
                        std::move(generateNonceFunction)) {
    }
};

/**
 * Implementation of the class that provides the client side implementation
 * of the SCRAM-SHA[1,256,512]
 */
class ClientBackend : public cb::sasl::client::MechanismBackend,
                      public ScramShaBackend {
public:
    ClientBackend(client::GetUsernameCallback& user_cb,
                  client::GetPasswordCallback& password_cb,
                  client::ClientContext& ctx,
                  Mechanism mechanism,
                  cb::crypto::Algorithm algo,
                  std::function<std::string()> generateNonceFunction);

    std::pair<Error, std::string_view> start() override;
    std::pair<Error, std::string_view> step(std::string_view input) override;

    std::string getName() const final {
        return ::to_string(mechanism);
    }

protected:
    std::string getStoredKey() override;
    std::string getServerKey() override;
    std::string getClientKey();
    std::string getClientProof();

    bool generateSaltedPassword(const std::string& secret);

    std::string getSaltedPassword() {
        if (saltedPassword.empty()) {
            throw std::logic_error(
                    "getSaltedPassword called before salted "
                    "password is initialized");
        }
        return saltedPassword;
    }

    std::string saltedPassword;
    std::string salt;
    unsigned int iterationCount = 4096;
};

class Sha512ClientBackend : public ClientBackend {
public:
    Sha512ClientBackend(client::GetUsernameCallback& user_cb,
                        client::GetPasswordCallback& password_cb,
                        client::ClientContext& ctx,
                        std::function<std::string()> generateNonceFunction = {})
        : ClientBackend(user_cb,
                        password_cb,
                        ctx,
                        Mechanism::SCRAM_SHA512,
                        cb::crypto::Algorithm::SHA512,
                        std::move(generateNonceFunction)) {
    }
};

class Sha256ClientBackend : public ClientBackend {
public:
    Sha256ClientBackend(client::GetUsernameCallback& user_cb,
                        client::GetPasswordCallback& password_cb,
                        client::ClientContext& ctx,
                        std::function<std::string()> generateNonceFunction = {})
        : ClientBackend(user_cb,
                        password_cb,
                        ctx,
                        Mechanism::SCRAM_SHA256,
                        cb::crypto::Algorithm::SHA256,
                        std::move(generateNonceFunction)) {
    }
};

class Sha1ClientBackend : public ClientBackend {
public:
    Sha1ClientBackend(client::GetUsernameCallback& user_cb,
                      client::GetPasswordCallback& password_cb,
                      client::ClientContext& ctx,
                      std::function<std::string()> generateNonceFunction = {})
        : ClientBackend(user_cb,
                        password_cb,
                        ctx,
                        Mechanism::SCRAM_SHA1,
                        cb::crypto::Algorithm::SHA1,
                        std::move(generateNonceFunction)) {
    }
};

} // namespace cb::sasl::mechanism::scram
