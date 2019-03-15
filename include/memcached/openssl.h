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

#include <memory>

#ifdef WIN32
// The OpenSSL headers end up including winsock.h causing conflicting
// types in programs depending on the order the include file is being
// used. To work around those files being included we'll include winsock2.h
// here.
#include <winsock2.h>
#endif
#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/err.h>

namespace cb {
namespace openssl {
struct X509deletor {
    void operator()(X509* cert) {
        X509_free(cert);
    }
};

using unique_x509_ptr = std::unique_ptr<X509, X509deletor>;
}
}

