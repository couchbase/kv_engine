/*
 *     Copyright 2013 Couchbase, Inc.
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

#include <platform/platform.h>
#include "cbsasl/cbsasl.h"
#include "cbsasl/cbsasl_internal.h"
#include "util.h"
#include <stdlib.h>

CBSASL_PUBLIC_API
void cbsasl_dispose(cbsasl_conn_t** conn) {
    if (conn != nullptr) {
        delete *conn;
        *conn = nullptr;
    }
}

static const char* hexchar = "0123456789abcdef";

void cbsasl_hex_encode(char* dest, const char* src, size_t srclen) {
    size_t ii;
    for (ii = 0; ii < srclen; ii++) {
        dest[ii * 2] = hexchar[(src[ii] >> 4) & 0xF];
        dest[ii * 2 + 1] = hexchar[src[ii] & 0xF];
    }
}

static const bool use_saslauthd{getenv("CBAUTH_SOCKPATH") != nullptr};

bool cbsasl_use_saslauthd() {
    return use_saslauthd;
}
