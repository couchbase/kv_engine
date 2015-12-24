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

#include <array>
#include <vector>
#include "cbsasl/cbsasl.h"
#include "cbsasl/cbsasl_internal.h"

#define MECH_NAME_CRAM_MD5 "CRAM-MD5"
#define DIGEST_LENGTH 16

class CramMd5ServerBackend : public MechanismBackend {
public:
    CramMd5ServerBackend();

    virtual cbsasl_error_t start(cbsasl_conn_t* conn, const char* input,
                                 unsigned inputlen,
                                 const char** output,
                                 unsigned* outputlen) override;

    virtual cbsasl_error_t step(cbsasl_conn_t* conn, const char* input,
                                unsigned inputlen, const char** output,
                                unsigned* outputlen) override;

private:
    std::array<char, DIGEST_LENGTH> digest;
};

class CramMd5ClientBackend : public MechanismBackend {
public:
    CramMd5ClientBackend()
        : MechanismBackend(MECH_NAME_CRAM_MD5) {

    }

    virtual cbsasl_error_t start(cbsasl_conn_t* conn, const char* input,
                                 unsigned inputlen,
                                 const char** output,
                                 unsigned* outputlen) override;

    virtual cbsasl_error_t step(cbsasl_conn_t* conn, const char* input,
                                unsigned inputlen, const char** output,
                                unsigned* outputlen) override;

private:
    std::vector<char> buffer;
};
