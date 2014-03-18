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
#include "hmac.h"
#include "cbsasl/pwfile.h"
#include "cbsasl/util.h"
#include <string.h>
#include <stdlib.h>

#define NONCE_LENGTH 8

cbsasl_error_t cram_md5_server_init() {
    return SASL_OK;
}

cbsasl_error_t cram_md5_server_start(cbsasl_conn_t* conn) {
    /* Generate a challenge */
    char nonce[NONCE_LENGTH];
    if (cbsasl_secure_random(nonce, NONCE_LENGTH) != SASL_OK) {
        return SASL_FAIL;
    }

    conn->c.server.sasl_data = malloc(NONCE_LENGTH * 2);
    if(conn->c.server.sasl_data == NULL) {
        return SASL_FAIL;
    }

    conn->c.server.sasl_data_len = NONCE_LENGTH * 2;
    cbsasl_hex_encode(conn->c.server.sasl_data, nonce, NONCE_LENGTH);
    return SASL_CONTINUE;
}

cbsasl_error_t cram_md5_server_step(cbsasl_conn_t *conn,
                                    const char *input,
                                    unsigned inputlen,
                                    const char **output,
                                    unsigned *outputlen)
{
    unsigned int userlen;
    char *user;
    char *cfg;
    char *pass;
    unsigned char digest[DIGEST_LENGTH];
    char md5string[DIGEST_LENGTH * 2];

    if (inputlen <= 33) {
        return SASL_BADPARAM;
    }

    userlen = inputlen - (DIGEST_LENGTH * 2) - 1;
    user = calloc((userlen + 1) * sizeof(char), 1);
    memcpy(user, input, userlen);
    user[userlen] = '\0';

    pass = find_pw(user, &cfg);
    if (pass == NULL) {
        return SASL_FAIL;
    }

    hmac_md5((unsigned char *)conn->c.server.sasl_data,
             conn->c.server.sasl_data_len,
             (unsigned char *)pass,
             strlen(pass), digest);

    cbsasl_hex_encode(md5string, (char *) digest, DIGEST_LENGTH);

    if (cbsasl_secure_compare(md5string,
                              (DIGEST_LENGTH * 2),
                              &(input[userlen + 1]),
                              (DIGEST_LENGTH * 2)) != 0) {
        return SASL_FAIL;
    }

    conn->c.server.username = user;
    conn->c.server.config = strdup(cfg);
    *output = NULL;
    *outputlen = 0;
    return SASL_OK;
}

cbsasl_mechs_t get_cram_md5_mechs(void)
{
    static cbsasl_mechs_t mechs = {
        MECH_NAME_CRAM_MD5,
        cram_md5_server_init,
        cram_md5_server_start,
        cram_md5_server_step
    };
    return mechs;
}
