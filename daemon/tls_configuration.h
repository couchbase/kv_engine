/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once
#include "ssl_utils.h"
#include <memcached/openssl.h>
#include <nlohmann/json.hpp>
#include <stdexcept>

class CreateSslContextException : public std::runtime_error {
public:
    CreateSslContextException(std::string_view message,
                              std::string_view function,
                              nlohmann::json ssl_error)
        : CreateSslContextException(
                  format(message, function, std::move(ssl_error))) {
    }
    const nlohmann::json error;

protected:
    explicit CreateSslContextException(nlohmann::json error)
        : std::runtime_error(error.dump()), error(std::move(error)) {
    }

    static nlohmann::json format(std::string_view message,
                                 std::string_view function,
                                 nlohmann::json error) {
        return nlohmann::json{{"message", message},
                              {"function", function},
                              {"error", std::move(error)}};
    }
};

class TlsConfiguration {
public:
    enum class ClientCertMode { Mandatory, Enabled, Disabled };
    /**
     * Create a new instance of the TLS configuration (this would
     * create an SSL_CTX to use to create new SSL objects. Use validate()
     * if you just want to validate the format and not load any files)
     *
     * @param spec The specification for the JSON to use
     * @throws std::invalid_argument for format errors
     * @throws CreateSslContextException for SSL related errors
     */
    explicit TlsConfiguration(const nlohmann::json& spec);

    nlohmann::json to_json() const;
    uniqueSslPtr createClientSslHandle();

    /// Validate that the json spec meets the requirement (required keys
    /// and correct types). Note that this function does NOT try to load
    /// any files or check the password.
    ///
    /// The validate method should be called from the packet validator
    /// to check the message before accepting the call
    ///
    /// @param spec the JSON to check
    /// @throws std::invalid_argument if it fails to meet the criterias
    static void validate(const nlohmann::json& spec);

protected:
    /// Create the OpenSSL Server context structure
    cb::openssl::unique_ssl_ctx_ptr createServerContext(
            const nlohmann::json& spec);

    const std::string private_key;
    const std::string certificate_chain;
    const std::string ca_file;
    const bool password; // We don't store the password in memory
    const std::string minimum_version;
    const std::string cipher_list;
    const std::string cipher_suites;
    const bool cipher_order;
    const ClientCertMode clientCertMode;

    cb::openssl::unique_ssl_ctx_ptr serverContext;
};

TlsConfiguration::ClientCertMode from_string(std::string_view view);
std::string to_string(const TlsConfiguration::ClientCertMode cp);
std::ostream& operator<<(std::ostream& os,
                         const TlsConfiguration::ClientCertMode& cp);
