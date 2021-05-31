/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "tls_configuration.h"
#include "ssl_utils.h"
#include <nlohmann/json.hpp>
#include <platform/base64.h>

std::string getString(const nlohmann::json& spec,
                      const std::string key,
                      bool optional = false) {
    auto iter = spec.find(key);
    if (iter == spec.cend()) {
        if (!optional) {
            throw std::runtime_error("TLS configuration must contain \"" + key +
                                     "\" which must be a string");
        }
        return {};
    }

    if (!iter->is_string()) {
        throw std::runtime_error("TLS configuration for \"" + key +
                                 "\" must be a string");
    }

    return iter->get<std::string>();
}

std::string getCipherList(const nlohmann::json& spec,
                          const std::string key,
                          bool optional = false) {
    auto cipher = spec.find("cipher list");
    if (cipher == spec.cend()) {
        if (!optional) {
            throw std::runtime_error(
                    "TLS configuration must contain \"cipher list\" which must "
                    "be an object");
        }
        return {};
    }

    auto iter = cipher->find(key);
    if (iter == cipher->cend()) {
        if (!optional) {
            throw std::runtime_error(
                    R"(The TLS configurations "cipher list" section must contain ")" +
                    key + "\" which must be a string");
        }
        return {};
    }

    if (!iter->is_string()) {
        throw std::runtime_error(
                R"(The TLS configurations "cipher list" for ")" + key +
                "\" must be a string");
    }

    return iter->get<std::string>();
}

bool getBoolean(const nlohmann::json& spec, const std::string key) {
    auto iter = spec.find(key);
    if (iter == spec.cend()) {
        throw std::runtime_error("TLS configuration must contain \"" + key +
                                 "\" which must be a boolean value");
    }

    if (!iter->is_boolean()) {
        throw std::runtime_error("TLS configuration for \"" + key +
                                 "\" must be a boolean value");
    }

    return iter->get<bool>();
}

nlohmann::json TlsConfiguration::to_json() const {
    return {{"private key", private_key},
            {"certificate chain", certificate_chain},
            {"CA file", ca_file},
            {"password", password ? "set" : "not set"},
            {"minimum version", minimum_version},
            {"cipher list",
             {{"tls 1.2", cipher_list}, {"tls 1.3", cipher_suites}}},
            {"cipher order", cipher_order},
            {"client cert auth", to_string(clientCertMode)}};
}

TlsConfiguration::TlsConfiguration(const nlohmann::json& spec)
    : private_key(getString(spec, "private key")),
      certificate_chain(getString(spec, "certificate chain")),
      ca_file(getString(spec, "CA file", true)),
      password(!getString(spec, "password", true).empty()),
      minimum_version(getString(spec, "minimum version")),
      cipher_list(getCipherList(spec, "tls 1.2")),
      cipher_suites(getCipherList(spec, "tls 1.3", false)),
      cipher_order(getBoolean(spec, "cipher order")),
      clientCertMode(from_string(getString(spec, "client cert auth"))),
      serverContext(createServerContext(spec)) {
}

static int my_pem_password_cb(char* buf, int size, int, void* userdata) {
    if (!userdata) {
        throw std::runtime_error("my_pem_password_cb called without userdata");
    }
    auto* json = static_cast<nlohmann::json*>(userdata);
    auto iter = json->find("password");
    if (iter == json->end()) {
        // The password _SHOULD_ be there
        throw std::invalid_argument(
                "TlsConfiguration: Failed to locate password");
    }
    auto base64 = iter->get<std::string>();
    auto decoded = cb::base64::decode(base64);
    if (decoded.size() > std::size_t(size)) {
        throw std::runtime_error("TlsConfiguration: Password is too long");
    }
    std::copy(decoded.begin(), decoded.end(), buf);
    return decoded.size();
}

cb::openssl::unique_ssl_ctx_ptr TlsConfiguration::createServerContext(
        const nlohmann::json& spec) {
    cb::openssl::unique_ssl_ctx_ptr ret{SSL_CTX_new(SSLv23_server_method())};
    auto* server_ctx = ret.get();
    SSL_CTX_set_dh_auto(server_ctx, 1);
    long options = decode_ssl_protocol(minimum_version);
    if (cipher_order) {
        options |= SSL_OP_CIPHER_SERVER_PREFERENCE;
    }
    SSL_CTX_set_options(server_ctx, options);
    SSL_CTX_set_mode(server_ctx,
                     SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |
                             SSL_MODE_ENABLE_PARTIAL_WRITE);

    if (!ca_file.empty() &&
        !SSL_CTX_load_verify_locations(server_ctx, ca_file.c_str(), nullptr)) {
        throw std::runtime_error("SSL_CTX_load_verify_locations failed");
    }

    if (password) {
        SSL_CTX_set_default_passwd_cb(server_ctx, my_pem_password_cb);
        SSL_CTX_set_default_passwd_cb_userdata(
                server_ctx, const_cast<void*>(static_cast<const void*>(&spec)));
    }

    if (!SSL_CTX_use_certificate_chain_file(server_ctx,
                                            certificate_chain.c_str()) ||
        !SSL_CTX_use_PrivateKey_file(
                server_ctx, private_key.c_str(), SSL_FILETYPE_PEM)) {
        throw std::runtime_error("Failed to enable ssl!");
    }
    if (password) {
        // This might not be necessary, just to make sure that we don't
        // try to use the userdata we set previously in the SSL instance
        // created from this ssl context (because we don't want to keep
        // the password stored in memory)
        SSL_CTX_set_default_passwd_cb_userdata(server_ctx, nullptr);
    }

    set_ssl_ctx_ciphers(server_ctx, cipher_list.c_str(), cipher_suites.c_str());
    int ssl_flags = 0;
    switch (clientCertMode) {
    case ClientCertMode::Mandatory:
        ssl_flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        // FALLTHROUGH
    case ClientCertMode::Enabled: {
        ssl_flags |= SSL_VERIFY_PEER;
        auto* certNames = SSL_load_client_CA_file(certificate_chain.c_str());
        if (certNames == nullptr) {
            std::string msg = "Failed to read SSL cert " + certificate_chain;
            throw std::runtime_error(msg);
        }
        SSL_CTX_set_client_CA_list(server_ctx, certNames);
        SSL_CTX_load_verify_locations(
                server_ctx, certificate_chain.c_str(), nullptr);
        SSL_CTX_set_verify(server_ctx, ssl_flags, nullptr);
        break;
    }
    case ClientCertMode::Disabled:
        break;
    }

    return ret;
}

uniqueSslPtr TlsConfiguration::createClientSslHandle() {
    return uniqueSslPtr{SSL_new(serverContext.get())};
}

TlsConfiguration::ClientCertMode from_string(std::string_view view) {
    if (view == "mandatory") {
        return TlsConfiguration::ClientCertMode::Mandatory;
    }
    if (view == "enabled") {
        return TlsConfiguration::ClientCertMode::Enabled;
    }
    if (view == "disabled") {
        return TlsConfiguration::ClientCertMode::Disabled;
    }
    throw std::invalid_argument("from_string: Unknown Client Cert Mode: " +
                                std::string(view));
}

std::string to_string(TlsConfiguration::ClientCertMode ccm) {
    switch (ccm) {
    case TlsConfiguration::ClientCertMode::Mandatory:
        return "mandatory";
    case TlsConfiguration::ClientCertMode::Enabled:
        return "enabled";
    case TlsConfiguration::ClientCertMode::Disabled:
        return "disabled";
    }
    throw std::invalid_argument("Invalid TlsConfiguration::ClientCertMode: " +
                                std::to_string(int(ccm)));
}

std::ostream& operator<<(std::ostream& os,
                         const TlsConfiguration::ClientCertMode& ccm) {
    return os << to_string(ccm);
}
