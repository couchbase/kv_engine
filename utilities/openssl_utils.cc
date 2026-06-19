/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "openssl_utils.h"

#include "crl_policy.h"

#include <fmt/format.h>
#include <gsl/gsl-lite.hpp>
#include <openssl/pem.h>
#include <openssl/x509.h>

namespace cb::openssl {

void X509deletor::operator()(X509* cert) {
    X509_free(cert);
}

void SSL_CTX_Deletor::operator()(SSL_CTX* ctx) {
    SSL_CTX_free(ctx);
}

void BIO_Deleter::operator()(BIO* bio) {
    BIO_free(bio);
}

void X509_CRL_Deleter::operator()(X509_CRL* crl) const {
    X509_CRL_free(crl);
}

CreateSslContextException::CreateSslContextException(std::string_view message,
                                                     std::string_view function,
                                                     nlohmann::json ssl_error)
    : CreateSslContextException(
              format(message, function, std::move(ssl_error))) {
}

CreateSslContextException::CreateSslContextException(nlohmann::json context)
    : std::runtime_error(context.dump()), error(std::move(context)) {
}

nlohmann::json CreateSslContextException::format(std::string_view message,
                                                 std::string_view function,
                                                 nlohmann::json error) {
    return nlohmann::json{{"message", message},
                          {"function", function},
                          {"error", std::move(error)}};
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

void loadCrlFromMemory(X509_STORE* store, std::string_view pem) {
    Expects(store && "Store must be set");
    if (pem.empty()) {
        throw std::runtime_error("PEM data must not be empty");
    }

    unique_bio_ptr bio(
            BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())));
    if (!bio) {
        throw CreateSslContextException("Failed to create memory BIO.",
                                        "BIO_new_mem_buf",
                                        getOpenSslError());
    }

    // PEM files can contain multiple appended CRL entries. Loop until the end.
    bool loadedAtLeastOne = false;
    while (true) {
        // Read a single CRL layout structure out of the current position in the
        // stream
        X509_CRL* rawCrl =
                PEM_read_bio_X509_CRL(bio.get(), nullptr, nullptr, nullptr);
        if (!rawCrl) {
            // Check if we hit the natural EOF or if it's an actual parsing
            // break
            unsigned long err = ERR_peek_last_error();
            if (ERR_GET_LIB(err) == ERR_LIB_PEM &&
                ERR_GET_REASON(err) == PEM_R_NO_START_LINE) {
                ERR_clear_error(); // Clean up expected EOF flag
                break;
            }
            throw CreateSslContextException("Failed to parse CRL from PEM data",
                                            "PEM_read_bio_X509_CRL",
                                            getOpenSslError());
        }

        unique_x509_crl_ptr crl(rawCrl);

        // Push the parsed structure safely into the store memory cache
        if (X509_STORE_add_crl(store, crl.get()) != 1) {
            throw CreateSslContextException(
                    "Failed to add parsed CRL to store cache",
                    "X509_STORE_add_crl",
                    getOpenSslError());
        }
        loadedAtLeastOne = true;
    }
    if (!loadedAtLeastOne) {
        throw std::runtime_error("Failed to load any CRLs from the file");
    }
}

/// Extract subject, issuer and serial from a certificate for use in audit
/// events. Returns an empty optional if the cert pointer is null.
static std::optional<nlohmann::json> peer_certificate_info(X509* cert) {
    if (!cert) {
        return std::nullopt;
    }
    nlohmann::json info;
    std::array<char, 256> buf;

    auto* subject = X509_get_subject_name(cert);
    if (subject &&
        X509_NAME_oneline(subject, buf.data(), buf.size()) != nullptr) {
        info["subject"] = buf.data();
    }

    auto* issuer = X509_get_issuer_name(cert);
    if (issuer &&
        X509_NAME_oneline(issuer, buf.data(), buf.size()) != nullptr) {
        info["issuer"] = buf.data();
    }

    auto* serial = X509_get_serialNumber(cert);
    if (serial) {
        BIGNUM* bn = ASN1_INTEGER_to_BN(serial, nullptr);
        if (bn) {
            char* hex = BN_bn2hex(bn);
            if (hex) {
                info["serial"] = hex;
                OPENSSL_free(hex);
            }
            BN_free(bn);
        }
    }

    return info;
}

int crlPolicyVerifyCallback(
        int preverify_ok,
        CrlPolicy activePolicy,
        X509_STORE_CTX* x509_ctx,
        const CrlPolicyVerificationIssueCallback& errorCallback) {
    const auto error = X509_STORE_CTX_get_error(x509_ctx);
    const auto certInfo =
            peer_certificate_info(X509_STORE_CTX_get_current_cert(x509_ctx));
    const auto* errorStr = X509_verify_cert_error_string(error);

    if (activePolicy == CrlPolicy::Disabled) {
        // CRL checking is disabled for this scope: ignore all CRL-related
        // errors including explicit revocation, so that even a revoked
        // certificate is accepted (per PRD: Disabled → revoked cert: Allow)
        if (error == X509_V_ERR_UNABLE_TO_GET_CRL ||
            error == X509_V_ERR_CRL_HAS_EXPIRED ||
            error == X509_V_ERR_CRL_SIGNATURE_FAILURE ||
            error == X509_V_ERR_CERT_REVOKED) {
            X509_STORE_CTX_set_error(x509_ctx, X509_V_OK);
            return 1;
        }
    }

    if (activePolicy == CrlPolicy::Permissive) {
        // Allow missing or expired CRLs, but maintain hard drops for outright
        // bad signatures
        if (error == X509_V_ERR_UNABLE_TO_GET_CRL ||
            error == X509_V_ERR_CRL_HAS_EXPIRED) {
            errorCallback(false, errorStr, certInfo);
            X509_STORE_CTX_set_error(x509_ctx, X509_V_OK);
            return 1;
        }
        // Explicit revocation is always enforced even in Permissive mode.
        if (error == X509_V_ERR_CERT_REVOKED) {
            errorCallback(true, errorStr, certInfo);
            return 0;
        }
    }

    if (activePolicy == CrlPolicy::Strict) {
        // Missing CRL: allow with a warning (same as Permissive for this case)
        if (error == X509_V_ERR_UNABLE_TO_GET_CRL) {
            errorCallback(false, errorStr, certInfo);
            X509_STORE_CTX_set_error(x509_ctx, X509_V_OK);
            return 1;
        }
        // Expired CRL or revoked certificate: reject
        if (error == X509_V_ERR_CRL_HAS_EXPIRED ||
            error == X509_V_ERR_CERT_REVOKED) {
            errorCallback(true, errorStr, certInfo);
            return 0;
        }
    }

    if (activePolicy == CrlPolicy::Require) {
        // If OpenSSL found an explicit revocation record, block immediately
        if (error == X509_V_ERR_CERT_REVOKED) {
            errorCallback(true, errorStr, certInfo);
            return 0; // Hard drop
        }
        // CRL infrastructure failure (missing, expired, or bad signature):
        // preverify_ok is already 0 so the connection will be rejected, but
        // log the reason so operators can distinguish a CRL availability
        // problem from a legitimate revocation rejection.
        if (error == X509_V_ERR_UNABLE_TO_GET_CRL ||
            error == X509_V_ERR_CRL_HAS_EXPIRED ||
            error == X509_V_ERR_CRL_SIGNATURE_FAILURE) {
            errorCallback(true, errorStr, certInfo);
        }
    }

    // Fall back to OpenSSL's native judgment for non-CRL failures (e.g.,
    // expired cert, bad chain)
    return preverify_ok;
}

static const int ssl_ctx_crl_policy_ex_data_index =
        SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);

static const int ssl_ctx_memcached_connection_ex_data_index =
        SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);

unique_ssl_ctx_ptr createServerSideSslContext(CrlPolicy policy) {
    unique_ssl_ctx_ptr ctx{SSL_CTX_new(TLS_server_method())};
    if (!ctx) {
        return {};
    }

    if (!SSL_CTX_set_ex_data(ctx.get(),
                             ssl_ctx_crl_policy_ex_data_index,
                             (void*)(intptr_t)policy)) {
        throw CreateSslContextException(
                "Failed to store policy to the context object",
                "SSL_CTX_set_ex_data",
                getOpenSslError());
    }
    return ctx;
}

unique_ssl_ctx_ptr createClientSideSslContext(MemcachedConnection* connection) {
    unique_ssl_ctx_ptr ctx{SSL_CTX_new(TLS_client_method())};
    if (!ctx) {
        return {};
    }

    if (!SSL_CTX_set_ex_data(ctx.get(),
                             ssl_ctx_memcached_connection_ex_data_index,
                             connection)) {
        throw CreateSslContextException(
                "Failed to store MemcachedConnection to the context object",
                "SSL_CTX_set_ex_data",
                getOpenSslError());
    }
    return ctx;
}

CrlPolicy getCrlPolicy(SSL_CTX* ctx) {
    Expects(ctx);
    void* data = SSL_CTX_get_ex_data(ctx, ssl_ctx_crl_policy_ex_data_index);
    return static_cast<CrlPolicy>(reinterpret_cast<intptr_t>(data));
}

MemcachedConnection* getMemcachedConnection(SSL_CTX* ctx) {
    Expects(ctx);
    return reinterpret_cast<MemcachedConnection*>(SSL_CTX_get_ex_data(
            ctx, ssl_ctx_memcached_connection_ex_data_index));
}

} // namespace cb::openssl
