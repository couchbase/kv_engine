/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

// The OpenSSL headers end up including winsock.h causing conflicting
// types in programs depending on the order the include file is being
// used. To work around those files being included we'll include winsock2.h
// here via folly's Windows.h here.
#include <folly/portability/Windows.h>

#include <memory>

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/err.h>

namespace cb::openssl {
struct X509deletor {
    void operator()(X509* cert) {
        X509_free(cert);
    }
};

using unique_x509_ptr = std::unique_ptr<X509, X509deletor>;

struct SSL_CTX_Deletor {
    void operator()(SSL_CTX* ctx) {
        SSL_CTX_free(ctx);
    }
};

using unique_ssl_ctx_ptr = std::unique_ptr<SSL_CTX, SSL_CTX_Deletor>;
} // namespace cb::openssl
