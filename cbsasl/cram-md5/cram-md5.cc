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
#include "cram-md5.h"
#include "cbsasl/pwfile.h"
#include "cbsasl/util.h"
#include <string.h>
#include <stdlib.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#define NONCE_LENGTH 8
#define DIGEST_LENGTH 16

CramMd5ServerBackend::CramMd5ServerBackend()
    : MechanismBackend(MECH_NAME_CRAM_MD5) {
    /* Generate a challenge */
    std::array<char, NONCE_LENGTH> nonce;
    if (cbsasl_secure_random(nonce.data(), nonce.size()) != CBSASL_OK) {
        throw std::bad_alloc();
    }

    cbsasl_hex_encode(digest.data(), nonce.data(), nonce.size());
}

cbsasl_error_t CramMd5ServerBackend::start(cbsasl_conn_t* conn,
                                           const char* input,
                                           unsigned inputlen,
                                           const char** output,
                                           unsigned* outputlen) {
    if (inputlen != 0 || output == nullptr || outputlen == nullptr) {
        return CBSASL_BADPARAM;
    }

    if (conn->get_cnonce_fn != nullptr) {
        // Allow the user to override the nonce
        const char *cnonce = nullptr;
        unsigned int len;

        if (conn->get_cnonce_fn(conn->get_cnonce_ctx, CBSASL_CB_CNONCE,
                                &cnonce, &len) != 0) {
            return CBSASL_FAIL;
        }

        if (len != DIGEST_LENGTH) {
            return CBSASL_BADPARAM;
        }

        memcpy(digest.data(), cnonce, len);
    }

    *output = digest.data();
    *outputlen = (unsigned)digest.size();

    return CBSASL_CONTINUE;
}

cbsasl_error_t CramMd5ServerBackend::step(cbsasl_conn_t* conn,
                                          const char* input,
                                          unsigned inputlen,
                                          const char** output,
                                          unsigned* outputlen) {
    unsigned int userlen;
    std::array<unsigned char, DIGEST_LENGTH> newdigest;
    std::array<char, DIGEST_LENGTH * 2> md5string;

    if (inputlen <= 33) {
        return CBSASL_BADPARAM;
    }

    userlen = inputlen - (DIGEST_LENGTH * 2) - 1;
    conn->server->username.assign(input, userlen);

    Couchbase::User user;
    if (!find_user(conn->server->username, user)) {
        return CBSASL_NOUSER;
    }

    std::string password;
    try {
        const auto& meta = user.getPassword(Mechanism::PLAIN);
        password.assign(meta.getPassword());
    } catch (...) {
        // There is no plain text password for the user
        return CBSASL_PWERR;
    }

    unsigned int digest_len;
    if (HMAC(EVP_md5(), (unsigned char*)password.data(),
             (int)password.length(),
             (unsigned char*)digest.data(), digest.size(),
             newdigest.data(), &digest_len) == NULL ||
        digest_len != DIGEST_LENGTH) {
        return CBSASL_PWERR;
    }

    cbsasl_hex_encode(md5string.data(), (const char*)newdigest.data(),
                      digest_len);

    if (cbsasl_secure_compare(md5string.data(), md5string.size(),
                              &(input[userlen + 1]),
                              (DIGEST_LENGTH * 2)) != 0) {
        return CBSASL_PWERR;
    }

    *output = nullptr;
    *outputlen = 0;
    return CBSASL_OK;
}

cbsasl_error_t CramMd5ClientBackend::start(cbsasl_conn_t* conn,
                                           const char* input,
                                           unsigned inputlen,
                                           const char** output,
                                           unsigned* outputlen) {

    if (output == nullptr || outputlen == nullptr) {
        return CBSASL_BADPARAM;
    }

    *output = nullptr;
    *outputlen = 0;

    return CBSASL_OK;
}

cbsasl_error_t CramMd5ClientBackend::step(cbsasl_conn_t* conn,
                                          const char* input,
                                          unsigned inputlen,
                                          const char** output,
                                          unsigned* outputlen) {


    const char* usernm = nullptr;
    unsigned int usernmlen;

    auto* client = conn->client.get();
    if (cbsasl_get_username(client->get_username, client->get_username_ctx,
                            &usernm, &usernmlen) != CBSASL_OK) {
        return CBSASL_FAIL;
    }

    cbsasl_secret_t* pass;
    if (cbsasl_get_password(client->get_password, conn,
                            client->get_password_ctx, &pass) != CBSASL_OK) {
        return CBSASL_FAIL;
    }

    std::array<char, DIGEST_LENGTH * 2> md5string;
    try {
        buffer.resize(usernmlen + 1 + md5string.size(), 1);
    } catch (std::bad_alloc&) {
        return CBSASL_NOMEM;
    }

    std::array<unsigned char, DIGEST_LENGTH> digest;
    unsigned int digest_len;
    if (HMAC(EVP_md5(), (unsigned char*)pass->data, pass->len,
             (unsigned char*)input, inputlen,
             digest.data(), &digest_len) == nullptr ||
        digest_len != digest.size()) {
        return CBSASL_FAIL;
    }

    cbsasl_hex_encode(md5string.data(), (char*)digest.data(), digest.size());

    memcpy(buffer.data(), usernm, usernmlen);
    buffer[usernmlen] = ' ';
    memcpy(buffer.data() + usernmlen + 1, md5string.data(), md5string.size());

    *output = buffer.data();
    *outputlen = unsigned(buffer.size());
    return CBSASL_CONTINUE;
}
