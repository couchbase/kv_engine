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

// The OpenSSL headers end up including winsock.h causing conflicting
// types in programs depending on the order the include file is being
// used. To work around those files being included we'll include winsock2.h
// here via folly's Windows.h here.
#include <folly/portability/Windows.h>

#include <nlohmann/json.hpp>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <memory>
#include <stdexcept>
#include <string_view>

enum class CrlPolicy;
class MemcachedConnection;

namespace cb::openssl {
struct X509deletor {
    void operator()(X509* cert);
};

using unique_x509_ptr = std::unique_ptr<X509, X509deletor>;

struct SSL_CTX_Deletor {
    void operator()(SSL_CTX* ctx);
};

using unique_ssl_ctx_ptr = std::unique_ptr<SSL_CTX, SSL_CTX_Deletor>;

struct BIO_Deleter {
    void operator()(BIO* bio);
};

using unique_bio_ptr = std::unique_ptr<BIO, BIO_Deleter>;

struct X509_CRL_Deleter {
    void operator()(X509_CRL* crl) const;
};

using unique_x509_crl_ptr = std::unique_ptr<X509_CRL, X509_CRL_Deleter>;

/**
 * Exception thrown when creation of an SSL context fails. The error details
 * are stored as a JSON object containing the message, the function where the
 * failure occurred, and the OpenSSL error queue at the time of the failure.
 */
class CreateSslContextException : public std::runtime_error {
public:
    /**
     * @param message Human-readable description of what failed
     * @param function Name of the function where the error was detected
     * @param ssl_error OpenSSL error information (e.g. from getSSLErrorJSON())
     */
    CreateSslContextException(std::string_view message,
                              std::string_view function,
                              nlohmann::json ssl_error);

    /**
     * The full error context as a JSON object with keys "message",
     * "function", and "error".
     */
    const nlohmann::json error;

protected:
    explicit CreateSslContextException(nlohmann::json context);

    static nlohmann::json format(std::string_view message,
                                 std::string_view function,
                                 nlohmann::json error);
};

/**
 * Drains the OpenSSL error queue and returns all pending errors as a JSON
 * array of strings.
 */
nlohmann::json getOpenSslError();

/**
 * Loads one or more CRLs in PEM format from a memory buffer into an
 * X509_STORE. PEM data may contain multiple concatenated CRL blocks; all
 * are loaded.
 *
 * @param store The target X509_STORE to populate.
 * @param pem PEM-encoded CRL data.
 * @throws std::runtime_error if pem is empty or contains no parseable CRL.
 * @throws CreateSslContextException on an OpenSSL parsing or insertion error.
 */
void loadCrlFromMemory(X509_STORE* store, std::string_view pem);

/**
 * Callback invoked by crlPolicyVerifyCallback whenever a CRL-related
 * verification issue is acted upon.
 *
 * @param rejected true if the connection is being rejected due to this issue,
 *                 false if the issue is being bypassed with a warning.
 * @param errorStr Human-readable OpenSSL error string for the issue.
 * @param certInfo Subject, issuer, and serial of the peer certificate, or
 *                 an empty optional if no certificate is available.
 */
using CrlPolicyVerificationIssueCallback =
        std::function<void(bool, const char*, std::optional<nlohmann::json>)>;

/**
 * Applies the given CRL policy to an in-progress OpenSSL certificate
 * verification and returns the verification result. This is intended to be
 * called from an OpenSSL verify callback (SSL_CTX_set_verify).
 *
 * CRL-related errors are handled according to @p activePolicy; all other
 * errors fall through to OpenSSL's native judgment via @p preverify_ok.
 * @p errorCallback is invoked for each CRL issue that is either bypassed
 * (as a warning) or causes rejection.
 *
 * @param preverify_ok The result passed by OpenSSL to the verify callback.
 * @param activePolicy The CRL policy to enforce.
 * @param x509_ctx The X509 store context for the current verification step.
 * @param errorCallback Invoked for each CRL verification issue encountered.
 * @return 1 to allow the connection, 0 to reject it.
 */
int crlPolicyVerifyCallback(
        int preverify_ok,
        CrlPolicy activePolicy,
        X509_STORE_CTX* x509_ctx,
        const CrlPolicyVerificationIssueCallback& errorCallback);

/**
 * Create a server-side SSL_CTX and store @p policy in its ex_data slot so
 * that it can be retrieved later via getCrlPolicy().
 *
 * @param policy The CRL policy to associate with the context.
 * @return A new SSL_CTX, or an empty pointer if SSL_CTX_new fails.
 * @throws CreateSslContextException if the ex_data cannot be stored.
 */
unique_ssl_ctx_ptr createServerSideSslContext(CrlPolicy policy);

/**
 * Create a client-side SSL_CTX and store @p connection in its ex_data slot
 * so that it can be retrieved later via getMemcachedConnection().
 *
 * @param connection The owning MemcachedConnection. Must outlive the returned
 *                   context.
 * @return A new SSL_CTX, or an empty pointer if SSL_CTX_new fails.
 * @throws CreateSslContextException if the ex_data cannot be stored.
 */
unique_ssl_ctx_ptr createClientSideSslContext(MemcachedConnection* connection);

/**
 * Retrieve the CRL policy stored in @p ctx by createSslContext(CrlPolicy).
 *
 * @param ctx A non-null SSL_CTX created by createSslContext(CrlPolicy).
 * @return The CRL policy associated with the context.
 */
CrlPolicy getCrlPolicy(SSL_CTX* ctx);

/**
 * Retrieve the MemcachedConnection pointer stored in @p ctx by
 * createSslContext(MemcachedConnection*).
 *
 * @param ctx A non-null SSL_CTX created by
 *            createSslContext(MemcachedConnection*).
 * @return The MemcachedConnection associated with the context, or nullptr if
 *         none was stored.
 */
MemcachedConnection* getMemcachedConnection(SSL_CTX* ctx);
} // namespace cb::openssl
