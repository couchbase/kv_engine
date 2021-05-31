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
#include <nlohmann/json_fwd.hpp>

class TlsConfiguration {
public:
    enum class ClientCertMode { Mandatory, Enabled, Disabled };
    explicit TlsConfiguration(const nlohmann::json& spec);

    nlohmann::json to_json() const;
    uniqueSslPtr createClientSslHandle();

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
