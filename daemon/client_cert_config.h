/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <folly/Synchronized.h>
#include <nlohmann/json_fwd.hpp>
#include <openssl/ossl_typ.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace cb::x509 {

enum class Mode {
    /**
     * Don't try to look at the certificate at all
     */
    Disabled,
    /**
     * If the client provides a certificate we should look at it,
     * if not that's ok as well. We do allow certificates we can't map
     * to a user.. those needs to do SASL.
     */
    Enabled,
    /**
     * Clients must provide a certificate, and it needs to be able
     * to map down to a Couchbase user. If the client doesn't supply
     * a certificate (or it doesn't map to a user) the client will
     * be disconnected.
     */
    Mandatory
};

enum class Status {
    /**
     * An error occurred
     */
    Error,
    /**
     * None of the tuples matched the information in the certificate
     */
    NoMatch,
    /**
     * No certificate present
     */
    NotPresent,
    /**
     * Successfully mapped the information in the certificate to
     * a username
     */
    Success
};

/**
 * ClientCertConfig contains the in-memory configuration used
 * for authentication used by certificates provided over SSL.
 */
class ClientCertConfig {
public:
    /**
     * Factory method to create an instance of the ClientCertificateConfig
     * by parsing the provided JSON.
     *
     * @param config the JSON providing the configuration
     * @return the newly created configuration
     * @throws nlohmann::json::exception for json parsing/missing attribute
     *         errors
     * @throws std::invalid_argument if the provided JSON isn't according
     *         to the specification.
     */
    static std::unique_ptr<ClientCertConfig> create(
            const nlohmann::json& config);

    /**
     * Try to look up a username by using the defined mappings
     *
     * @param cert the certificate to pick out the user from
     * @return The status and the username (if found)
     */
    std::pair<Status, std::string> lookupUser(X509* cert) const;

    /**
     * Get a textual representation of this configuration
     */
    std::string to_string() const;

    /*
     * The rest of the public interface in the class is used
     * by unit tests to verify that the class works as expected.
     *
     * It is not intended for normal use
     */

    struct Mapping {
        Mapping() = default;
        Mapping(std::string& path_, const nlohmann::json& obj);
        virtual ~Mapping() = default;
        virtual std::pair<Status, std::string> match(X509* cert) const;

        std::string matchPattern(const std::string& input) const;

        std::string path;
        std::string prefix;
        std::string delimiter;
    };

    size_t getNumMappings() const {
        return mappings.size();
    }

    const Mapping& getMapping(size_t index) const;

protected:
    ClientCertConfig() = default;
    explicit ClientCertConfig(const nlohmann::json& config);
    std::vector<std::unique_ptr<Mapping>> mappings;
};

/**
 * The ClientCertMapper allows multiple threads to operate
 * on a ClientCertConfiguration to perform username mappings
 * from the certificate. It provides read-write locks so that
 * some threads may reconfigure the conversion parameters
 * and others perform lookup without any problems
 */
class ClientCertMapper {
public:
    /**
     * Reconfigure the client certificate mapper to use a new
     * underlying configuration.
     *
     * @param next The new configuration to use
     */
    void reconfigure(std::unique_ptr<ClientCertConfig> next);

    /**
     * Try to look up a username by using the defined mappings
     *
     * @param cert the certificate to pick out the user from
     * @return The status and the username (if found)
     */
    std::pair<Status, std::string> lookupUser(X509* cert) const;

    /**
     * Get a textual representation of this configuration
     */
    std::string to_string() const;

protected:
    folly::Synchronized<std::unique_ptr<ClientCertConfig>> config;
};

} // namespace cb::x509
