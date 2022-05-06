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
#include <logger/logger.h>

std::string getString(const nlohmann::json& spec,
                      const std::string key,
                      bool optional = false) {
    auto iter = spec.find(key);
    if (iter == spec.cend()) {
        if (!optional) {
            throw std::invalid_argument("TLS configuration must contain \"" +
                                        key + "\" which must be a string");
        }
        return {};
    }

    if (!iter->is_string()) {
        throw std::invalid_argument("TLS configuration for \"" + key +
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
            throw std::invalid_argument(
                    "TLS configuration must contain \"cipher list\" which must "
                    "be an object");
        }
        return {};
    }

    auto iter = cipher->find(key);
    if (iter == cipher->cend()) {
        if (!optional) {
            throw std::invalid_argument(
                    R"(The TLS configurations "cipher list" section must contain ")" +
                    key + "\" which must be a string");
        }
        return {};
    }

    if (!iter->is_string()) {
        throw std::invalid_argument(
                R"(The TLS configurations "cipher list" for ")" + key +
                "\" must be a string");
    }

    return iter->get<std::string>();
}

std::string getTlsMinVersion(const nlohmann::json& spec) {
    auto val = getString(spec, "minimum version");
    for (const auto& v : std::vector<std::string>{
                 {"TLS 1"}, {"TLS 1.1"}, {"TLS 1.2"}, {"TLS 1.3"}}) {
        if (val == v) {
            return val;
        }
    }
    // backwards compat (Should be removed when we nuke the parameters
    // in memcached.json)
    if (val == "tlsv1") {
        return "TLS 1";
    }

    if (val == "tlsv1.1" || val == "tlsv1_1") {
        return "TLS 1.1";
    }
    if (val == "tlsv1.2" || val == "tlsv1_2") {
        return "TLS 1.2";
    }

    if (val == "tlsv1.3" || val == "tlsv1_3") {
        return "TLS 1.3";
    }

    throw std::invalid_argument(
            R"("minimum version" must be one of "TLS 1", "TLS 1.1", "TLS 1.2" or "TLS 1.3")");
}

bool getBoolean(const nlohmann::json& spec, const std::string key) {
    auto iter = spec.find(key);
    if (iter == spec.cend()) {
        throw std::invalid_argument("TLS configuration must contain \"" + key +
                                    "\" which must be a boolean value");
    }

    if (!iter->is_boolean()) {
        throw std::invalid_argument("TLS configuration for \"" + key +
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
             {{"TLS 1.2", cipher_list}, {"TLS 1.3", cipher_suites}}},
            {"cipher order", cipher_order},
            {"client cert auth", to_string(clientCertMode)}};
}

TlsConfiguration::TlsConfiguration(const nlohmann::json& spec)
    : private_key(getString(spec, "private key")),
      certificate_chain(getString(spec, "certificate chain")),
      ca_file(getString(spec, "CA file")),
      password(!getString(spec, "password", true).empty()),
      minimum_version(getTlsMinVersion(spec)),
      cipher_list(getCipherList(spec, "TLS 1.2")),
      cipher_suites(getCipherList(spec, "TLS 1.3", false)),
      cipher_order(getBoolean(spec, "cipher order")),
      clientCertMode(from_string(getString(spec, "client cert auth"))),
      serverContext(createServerContext(spec)) {
}

static int my_pem_password_cb(char* buf, int size, int, void* userdata) {
    if (!userdata) {
        throw std::logic_error("my_pem_password_cb called without userdata");
    }
    auto& password = *static_cast<std::string*>(userdata);
    if (password.size() > std::size_t(size)) {
        LOG_WARNING("The provided password is too long for OpenSSL: ({} > {})",
                    password.size(),
                    size);
        return 0;
    }

    std::copy(password.begin(), password.end(), buf);
    return password.size();
}

nlohmann::json getOpenSslError() {
    std::vector<std::string> ret;
    unsigned long code;
    while ((code = ERR_get_error()) != 0) {
        std::vector<char> ssl_err(1024);
        ERR_error_string_n(code, ssl_err.data(), ssl_err.size());
        ret.emplace_back(ssl_err.data());
    }
    return ret;
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

    if (!SSL_CTX_load_verify_locations(server_ctx, ca_file.c_str(), nullptr)) {
        throw CreateSslContextException("Failed to use: " + ca_file,
                                        "SSL_CTX_load_verify_locations",
                                        getOpenSslError());
    }

    auto iter = spec.find("password");
    std::string userpassword;
    if (iter != spec.end()) {
        auto base64 = iter->get<std::string>();
        auto decoded = cb::base64::decode(base64);
        std::copy(decoded.begin(),
                  decoded.end(),
                  std::back_inserter(userpassword));
    }

    // The validator should have already checked that the password must be
    // valid base64 encoded data. We _must_ have validated that _before_ doing
    // the callback as we cannot throw an exception from the callback handler as
    // that would cause OpenSSL to leak memory
    SSL_CTX_set_default_passwd_cb(server_ctx, my_pem_password_cb);
    SSL_CTX_set_default_passwd_cb_userdata(server_ctx,
                                           static_cast<void*>(&userpassword));

    if (!SSL_CTX_use_certificate_chain_file(server_ctx,
                                            certificate_chain.c_str())) {
        throw CreateSslContextException(
                "Failed to use certificate chain file: " + certificate_chain,
                "SSL_CTX_use_certificate_chain_file",
                getOpenSslError());
    }

    if (!SSL_CTX_use_PrivateKey_file(
                server_ctx, private_key.c_str(), SSL_FILETYPE_PEM)) {
        throw CreateSslContextException(
                "Failed to use private key file: " + private_key,
                "SSL_CTX_use_PrivateKey_file",
                getOpenSslError());
    }

    // This might not be necessary, just to make sure that we don't
    // try to use the userdata we set previously in the SSL instance
    // created from this ssl context (because we don't want to keep
    // the password stored in memory)
    SSL_CTX_set_default_passwd_cb_userdata(server_ctx, nullptr);
    set_ssl_ctx_ciphers(server_ctx, cipher_list.c_str(), cipher_suites.c_str());
    int ssl_flags = 0;
    switch (clientCertMode) {
    case ClientCertMode::Mandatory:
        ssl_flags |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
        // FALLTHROUGH
    case ClientCertMode::Enabled: {
        ssl_flags |= SSL_VERIFY_PEER;
        auto* certNames = SSL_load_client_CA_file(ca_file.c_str());
        if (!certNames) {
            throw CreateSslContextException(
                    "Failed to read SSL cert " + ca_file,
                    "SSL_load_client_CA_file",
                    getOpenSslError());
        }
        SSL_CTX_set_client_CA_list(server_ctx, certNames);
        SSL_CTX_load_verify_locations(server_ctx, ca_file.c_str(), nullptr);
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

void TlsConfiguration::validate(const nlohmann::json& spec) {
    getString(spec, "private key");
    getString(spec, "certificate chain");
    getString(spec, "CA file", true);
    const auto pw = getString(spec, "password", true);
    if (!pw.empty()) {
        cb::base64::decode(pw);
    }
    getTlsMinVersion(spec);
    getCipherList(spec, "TLS 1.2");
    getCipherList(spec, "TLS 1.3", false);
    getBoolean(spec, "cipher order");
    from_string(getString(spec, "client cert auth"));

    const std::vector<std::string> keys{{"private key"},
                                        {"certificate chain"},
                                        {"CA file"},
                                        {"password"},
                                        {"minimum version"},
                                        {"cipher list"},
                                        {"cipher order"},
                                        {"client cert auth"}};
    auto isLegalKey = [&keys](const std::string& key) {
        for (const auto& k : keys) {
            if (k == key) {
                return true;
            }
        }
        return false;
    };

    // verify that we only accept the legal keys
    for (const auto& kv : spec.items()) {
        if (!isLegalKey(kv.key())) {
            throw std::invalid_argument("Invalid key provided: " + kv.key());
        }
    }

    for (const auto& kv : spec["cipher list"].items()) {
        if (kv.key() != "TLS 1.2" && kv.key() != "TLS 1.3") {
            throw std::invalid_argument(
                    "Invalid key for cipher list provided: " + kv.key());
        }
    }
}
