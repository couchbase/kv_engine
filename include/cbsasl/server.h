/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#pragma once

#include <cbsasl/context.h>
#include <cbsasl/domain.h>
#include <cbsasl/error.h>

#include <platform/sized_buffer.h>
#include <functional>
#include <memory>
#include <utility>

namespace cb {
namespace sasl {
namespace server {

/**
 * Initializes the sasl server
 *
 * This function initializes the server by loading passwords from the cbsasl
 * password file. This function should only be called once.
 */
void initialize();

/**
 * close and release allocated resources
 */
void shutdown();

/**
 * List all of the mechanisms available in cbsasl
 */
std::string listmech();

/**
 * Refresh the internal data (this may result in loading password
 * databases etc)
 */
cb::sasl::Error refresh();

/**
 * Set the HMAC interation count to use.
 */
void set_hmac_iteration_count(int count);

/**
 * Set the salt to use for unknown users trying to log in. We dont' want
 * to reveal that the user doesn't exist by always returning a random
 * salt. Instead we're using a HMAC of the username and this salt as
 * the salt we're reporting back to the user.
 */
void set_scramsha_fallback_salt(const std::string& salt);

class ServerContext;

class MechanismBackend {
public:
    explicit MechanismBackend(ServerContext& ctx)
        : context(ctx), domain(Domain::Local) {
    }
    virtual ~MechanismBackend() = default;
    virtual std::pair<cb::sasl::Error, cb::const_char_buffer> start(
            cb::const_char_buffer input) = 0;
    virtual std::pair<cb::sasl::Error, cb::const_char_buffer> step(
            cb::const_char_buffer input) = 0;
    virtual std::string getName() const = 0;

    void setUsername(std::string username) {
        MechanismBackend::username = std::move(username);
    }

    const std::string& getUsername() const {
        return username;
    }

    void setDomain(Domain domain) {
        MechanismBackend::domain = domain;
    }

    Domain getDomain() const {
        return domain;
    }

protected:
    ServerContext& context;
    std::string username;
    Domain domain;
};

class ServerContext : public Context {
public:
    const std::string getName() const {
        return backend->getName();
    }

    const std::string& getUsername() const {
        return backend->getUsername();
    }

    void setDomain(Domain domain) {
        backend->setDomain(domain);
    }

    Domain getDomain() const {
        return backend->getDomain();
    }

    bool isInitialized() {
        return backend.get() != nullptr;
    }

    std::pair<cb::sasl::Error, cb::const_char_buffer> start(
            const std::string& mech,
            const std::string& available,
            cb::const_char_buffer input);

    std::pair<cb::sasl::Error, cb::const_char_buffer> step(
            cb::const_char_buffer input) {
        return backend->step(input);
    }

    void reset() {
        backend.reset();
        uuid.clear();
    }

protected:
    std::unique_ptr<MechanismBackend> backend;
    std::string uuid;
};

} // namespace server
} // namespace sasl
} // namespace cb
