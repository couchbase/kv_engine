/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "user.h"
#include <cbsasl/context.h>
#include <cbsasl/domain.h>
#include <cbsasl/error.h>
#include <memcached/rbac/privilege_database.h>
#include <functional>
#include <memory>
#include <utility>

namespace cb::sasl::server {

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
 * Reload the password database and iterate over the database before it
 * is installed.
 */
Error reload_password_database(
        const std::function<void(const pwdb::User&)>& usercallback);

class ServerContext;

class MechanismBackend {
public:
    explicit MechanismBackend(ServerContext& ctx) : context(ctx) {
    }
    virtual ~MechanismBackend() = default;
    virtual std::pair<cb::sasl::Error, std::string_view> start(
            std::string_view input) = 0;
    virtual std::pair<cb::sasl::Error, std::string_view> step(
            std::string_view input) = 0;
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

    cb::rbac::UserIdent getUser() const {
        return {username, domain};
    }

protected:
    ServerContext& context;
    std::string username;
    Domain domain = Domain::Local;
};

class ServerContext : public Context {
public:
    const std::string getName() const {
        return backend->getName();
    }

    cb::rbac::UserIdent getUser() const {
        return backend->getUser();
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

    std::pair<cb::sasl::Error, std::string_view> start(
            const std::string& mech,
            const std::string& available,
            std::string_view input);

    std::pair<cb::sasl::Error, std::string_view> step(std::string_view input) {
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

} // namespace cb::sasl::server
