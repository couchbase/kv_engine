/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "cbsasl/cbsasl.h"
#include "cbsasl/cbsasl_internal.h"
#include <vector>

#define MECH_NAME_PLAIN "PLAIN"

class PlainServerBackend : public MechanismBackend {
public:
    PlainServerBackend()
        : MechanismBackend(MECH_NAME_PLAIN) {

    }

    virtual cbsasl_error_t start(cbsasl_conn_t* conn, const char* input,
                                 unsigned inputlen,
                                 const char** output,
                                 unsigned* outputlen) override;
};

class PlainClientBackend : public MechanismBackend {
public:
    PlainClientBackend()
        : MechanismBackend(MECH_NAME_PLAIN) {

    }

    virtual cbsasl_error_t start(cbsasl_conn_t* conn, const char* input,
                                 unsigned inputlen,
                                 const char** output,
                                 unsigned* outputlen) override;

private:
    /**
     * Where to store the encoded string:
     * "\0username\0password"
     */
    std::vector<char> buffer;
};
