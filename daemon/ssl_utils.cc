/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "config.h"

#include <algorithm>
#include <cctype>
#include <openssl/ssl.h>
#include <stdexcept>
#include "ssl_utils.h"

long decode_ssl_protocol(const char* protocol) {
    /* MB-12359 - Disable SSLv2 & SSLv3 due to POODLE */
    long disallow = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;

    if (protocol == NULL) {
        return disallow;
    }

    std::string minimum(protocol);
    std::transform(minimum.begin(), minimum.end(), minimum.begin(), tolower);

    if (minimum.empty() || minimum == "tlsv1") {
        // nothing
    } else if (minimum == "tlsv1.1" || minimum == "tlsv1_1") {
        disallow |= SSL_OP_NO_TLSv1;
    } else if (minimum == "tlsv1.2" || minimum == "tlsv1_2") {
        disallow |= SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1;
    } else {
        return -1;
    }

    return disallow;
}
