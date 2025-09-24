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
 * List all the mechanisms available in cbsasl
 *
 * @param tls include mechanisms which require TLS
 */
std::string listmech(bool tls = true);

/**
 * Set if we should allow for use of an external auth service.
 * When enabled SCRAM-SHA will return NO_USER in start() if the
 * user isn't found instead of running a full SCRAM authentication
 * and eventually fail.
 */
void set_using_external_auth_service(bool value);

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
    virtual std::pair<Error, std::string> start(std::string_view input) = 0;
    virtual std::pair<Error, std::string> step(std::string_view input) = 0;
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
    ServerContext() = default;
    ServerContext(std::function<pwdb::User(const std::string&)> function)
        : lookup_user_function(std::move(function)) {
    }
    const std::string getName() const {
        return backend->getName();
    }

    cb::rbac::UserIdent getUser() const {
        return backend->getUser();
    }

    const std::string& getUsername() const {
        return backend->getUsername();
    }

    void setUsername(std::string user) {
        backend->setUsername(std::move(user));
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

    std::pair<Error, std::string> start(std::string_view mech,
                                        std::string_view available,
                                        std::string_view input);

    std::pair<Error, std::string> step(std::string_view input);

    void reset() {
        backend.reset();
        uuid.clear();
    }

    /**
     * Look up the provided user and popuate the user entry if found.
     *
     * This allows the various backends to look up a user in the server
     * context they run (useful where we link memcached core together
     * with an auth provider and want them to use different password
     * database)
     *
     * @param username The user to search for
     * @return A user object (may be a dummy object if the user don't exists)
     */
    [[nodiscard]] pwdb::User lookupUser(const std::string& username);

    /**
     * Should one bypass authentication and return "No User" if the user
     * don't exists, or should the authentcation continue and return
     * "No user" as part of the final step of authentication (For
     * mechanisms which involves multiple roundtrips between the
     * client and the server)
     */
    [[nodiscard]] bool bypassAuthForUnknownUsers() const;

    [[nodiscard]] auto getExternalServerContext() const {
        return external_server_context;
    }

    void setExternalServerContext(std::string ctx) {
        external_server_context = std::move(ctx);
    }

    Error validateUserToken(const std::string_view user,
                            std::string_view token) {
        if (validate_user_token_function) {
            return validate_user_token_function(user, token);
        }
        return Error::NO_USER;
    }

    void setValidateUserTokenFunction(
            std::function<Error(std::string_view, std::string_view)> func) {
        validate_user_token_function = std::move(func);
    }

protected:
    /// The external auth provider might want to store some context
    /// information between each request (in a start-step scenatio)
    std::string external_server_context;
    std::unique_ptr<MechanismBackend> backend;
    std::function<pwdb::User(const std::string&)> lookup_user_function;
    std::function<Error(std::string_view, std::string_view)>
            validate_user_token_function;
    std::string uuid;
};

} // namespace cb::sasl::server
