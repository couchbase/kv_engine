/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <memcached/protocol_binary.h>
#include "utilities/protocol2text.h"
#include <cbsasl/cbsasl.h>
#include <iostream>
#include <libmcbp/mcbp.h>
#include <memcached/protocol_binary.h>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <vector>

#include "utilities.h"

static const bool packet_dump = getenv("COUCHBASE_PACKET_DUMP") != nullptr;

void ensure_send(BIO* bio, const void* data, int nbytes) {
    int total = 0;

    while (total < nbytes) {
        int nw = BIO_write(bio, (const char*)data + total, nbytes - total);
        if (nw <= 0) {
            if (BIO_should_retry(bio) == 0) {
                fprintf(stderr, "Failed to write data\n");
                exit(EXIT_FAILURE);
            }
        } else {
            total += nw;
        }
    }
}

void ensure_recv(BIO* bio, void* data, int nbytes) {
    int total = 0;

    while (total < nbytes) {
        int nr = BIO_read(bio, (char*)data + total, nbytes - total);
        if (nr <= 0) {
            if (BIO_should_retry(bio) == 0) {
                fprintf(stderr, "Failed to read data\n");
                exit(EXIT_FAILURE);
            }
        } else {
            total += nr;
        }
    }
}

struct my_sasl_ctx {
    const char* username;
    cbsasl_secret_t* secret;
};

static int sasl_get_username(void* context, int id, const char** result,
                             unsigned int* len) {
    struct my_sasl_ctx* ctx = reinterpret_cast<struct my_sasl_ctx*>(context);
    if (!context || !result ||
        (id != CBSASL_CB_USER && id != CBSASL_CB_AUTHNAME)) {
        return CBSASL_BADPARAM;
    }

    *result = ctx->username;
    if (len) {
        *len = (unsigned int)strlen(*result);
    }

    return CBSASL_OK;
}

static int sasl_get_password(cbsasl_conn_t* conn, void* context, int id,
                             cbsasl_secret_t** psecret) {
    struct my_sasl_ctx* ctx = reinterpret_cast<struct my_sasl_ctx*>(context);
    if (!conn || !psecret || id != CBSASL_CB_PASS || ctx == NULL) {
        return CBSASL_BADPARAM;
    }

    *psecret = ctx->secret;
    return CBSASL_OK;
}

static void sendCommand(BIO* bio,
                        uint8_t opcode,
                        const std::string& key,
                        const std::string& value) {
    protocol_binary_request_no_extras req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = opcode;
    req.message.header.request.keylen = htons(key.length());
    req.message.header.request.bodylen = htonl(key.length() + value.length());

    std::vector<uint8_t> buffer(
        sizeof(req.bytes) + key.length() + value.length());
    memcpy(buffer.data(), req.bytes, sizeof(req.bytes));
    memcpy(buffer.data() + sizeof(req.bytes), key.data(), key.length());
    memcpy(buffer.data() + sizeof(req.bytes) + key.length(), value.data(),
           value.length());
    sendCommand(bio, buffer);
}

void sendCommand(BIO* bio, const std::vector<uint8_t>& buffer) {
    ensure_send(bio, buffer.data(), buffer.size());
    if (packet_dump) {
        Couchbase::MCBP::dump(buffer.data(), std::cerr);
    }
}

void readResponse(BIO* bio, std::vector<uint8_t>& buffer) {
    protocol_binary_response_no_extras res;
    ensure_recv(bio, res.bytes, sizeof(res.bytes));
    uint32_t bodylen = ntohl(res.message.header.response.bodylen);
    buffer.resize(sizeof(res.bytes) + bodylen);
    memcpy(buffer.data(), res.bytes, sizeof(res.bytes));
    ensure_recv(bio, buffer.data() + sizeof(res.bytes), bodylen);
    if (packet_dump) {
        Couchbase::MCBP::dump(buffer.data(), std::cerr);
    }

    auto* r = reinterpret_cast<protocol_binary_response_header*>(buffer.data());
    r->response.bodylen = ntohl(r->response.bodylen);
    r->response.keylen = ntohs(r->response.keylen);
    r->response.status = ntohs(r->response.status);
}

static bool sasl_listmech(BIO* bio, std::vector<char>& mecs) {
    sendCommand(bio, PROTOCOL_BINARY_CMD_SASL_LIST_MECHS, "", "");

    std::vector<uint8_t> response;
    readResponse(bio, response);
    auto* r = reinterpret_cast<protocol_binary_response_header*>(response.data());
    if (r->response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {

        mecs.resize(r->response.bodylen);
        memcpy(mecs.data(), response.data() + sizeof(r->bytes),
               r->response.bodylen);
        return true;
    } else {
        auto status = (protocol_binary_response_status)r->response.status;
        std::cerr << "SASL LIST MECHS failed: "
                  << memcached_status_2_text(status)
                  << std::endl;
        return false;
    }
}

int do_sasl_auth(BIO* bio, const char* user, const char* pass) {
    std::vector<char> mechs;
    if (!sasl_listmech(bio, mechs)) {
        // error message already printed
        return -1;
    }

    cbsasl_callback_t sasl_callbacks[4];
    struct my_sasl_ctx context;

    sasl_callbacks[0].id = CBSASL_CB_USER;
    sasl_callbacks[0].proc = (int (*)(void))&sasl_get_username;
    sasl_callbacks[0].context = &context;
    sasl_callbacks[1].id = CBSASL_CB_AUTHNAME;
    sasl_callbacks[1].proc = (int (*)(void))&sasl_get_username;
    sasl_callbacks[1].context = &context;
    sasl_callbacks[2].id = CBSASL_CB_PASS;
    sasl_callbacks[2].proc = (int (*)(void))&sasl_get_password;
    sasl_callbacks[2].context = &context;
    sasl_callbacks[3].id = CBSASL_CB_LIST_END;
    sasl_callbacks[3].proc = NULL;
    sasl_callbacks[3].context = NULL;

    context.username = user;
    size_t pwlen = (pass == nullptr) ? 0 : strlen(pass);
    std::vector<uint8_t> buffer(sizeof(context.secret->len) + pwlen + 10);
    context.secret = reinterpret_cast<cbsasl_secret_t*>(buffer.data());
    memcpy(context.secret->data, pass, pwlen);
    context.secret->len = pwlen;

    cbsasl_conn_t* client;
    cbsasl_error_t err;
    err = cbsasl_client_new(NULL, NULL, NULL, NULL, sasl_callbacks, 0, &client);
    if (err != CBSASL_OK) {
        std::cerr << "Failed to create a new sasl instance: " << err <<
        std::endl;
        return -1;
    }

    const char* data;
    unsigned int len;
    const char* chosenmech;
    err = cbsasl_client_start(client, mechs.data(), NULL, &data, &len,
                              &chosenmech);

    if (err != CBSASL_OK) {
        std::cerr << "Failed to start sasl client: " << err << std::endl;
        return -1;
    }

    sendCommand(bio, PROTOCOL_BINARY_CMD_SASL_AUTH,
                std::string(chosenmech, strlen(chosenmech)),
                std::string(data, len));

    std::vector<uint8_t> response;
    readResponse(bio, response);

    auto* rsp = reinterpret_cast<protocol_binary_response_header*>(response.data());

    while (rsp->response.status ==
           PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE) {
        int datalen = rsp->response.bodylen -
                      rsp->response.keylen -
                      rsp->response.extlen;

        int dataoffset = sizeof(rsp->bytes) +
                         rsp->response.keylen +
                         rsp->response.extlen;

        err = cbsasl_client_step(client,
                                 reinterpret_cast<char*>(rsp->bytes +
                                                         dataoffset),
                                 datalen,
                                 NULL, &data, &len);
        if (err != CBSASL_OK && err != CBSASL_CONTINUE) {
            std::cerr << "Authentication failed: " << err << std::endl;
            return -1;
        }

        sendCommand(bio, PROTOCOL_BINARY_CMD_SASL_STEP,
                    std::string(chosenmech, strlen(chosenmech)),
                    std::string(data, len));

        readResponse(bio, response);
        rsp = reinterpret_cast<protocol_binary_response_header*>(response.data());
    }

    cbsasl_dispose(&client);

    if (rsp->response.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        auto status = (protocol_binary_response_status)rsp->response.status;
        std::cerr << "Authentication failure: "
                  << memcached_status_2_text(status)
                  << std::endl;
        return -1;
    }

    return 0;
}


bool enable_tcp_nodelay(BIO *bio)
{
    protocol_binary_request_hello request;
    uint16_t feature = htons(PROTOCOL_BINARY_FEATURE_TCPNODELAY);
    const char useragent[] = "mctools";
    memset(&request, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_HELLO;
    request.message.header.request.keylen = htons(sizeof(useragent) - 1);
    request.message.header.request.bodylen = htonl(2 + sizeof(useragent) - 1);

    ensure_send(bio, &request, sizeof(request));
    ensure_send(bio, &useragent, sizeof(useragent) - 1);
    ensure_send(bio, &feature, sizeof(feature));

    protocol_binary_response_hello response;
    ensure_recv(bio, &response, sizeof(response.bytes));

    uint32_t vallen = ntohl(response.message.header.response.bodylen);
    std::vector<char> buffer(vallen);
    ensure_recv(bio, buffer.data(), vallen);

    uint16_t status = ntohs(response.message.header.response.status);
    bool ret = true;

    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "Failed to enable TCP NODELAY: %s\n", buffer.data());
        ret = false;
    }

    return ret;
}

void initialize_openssl(void) {
    CRYPTO_malloc_init();
    SSL_library_init();
    SSL_load_error_strings();
    ERR_load_BIO_strings();
    OpenSSL_add_all_algorithms();
}

void shutdown_openssl() {
    // Global OpenSSL cleanup:
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);
    ENGINE_cleanup();
    CONF_modules_unload(1);
    ERR_free_strings();
    EVP_cleanup();
    CRYPTO_cleanup_all_ex_data();

    // per-thread cleanup:
    ERR_remove_state(0);

    // Newer versions of openssl (1.0.2a) have a the function
    // SSL_COMP_free_compression_methods() to perform this;
    // however we arn't that new...
    sk_SSL_COMP_free(SSL_COMP_get_compression_methods());
}

int create_ssl_connection(SSL_CTX** ctx,
                          BIO** bio,
                          const char* host,
                          const char* port,
                          const char* user,
                          const char* pass,
                          int secure)
{
    char uri[1024];

    initialize_openssl();
    if ((*ctx = SSL_CTX_new(SSLv23_client_method())) == NULL) {
        fprintf(stderr, "Failed to create openssl client contex\n");
        return 1;
    }

    snprintf(uri, sizeof(uri), "%s:%s", host, port);

    if (secure) {
        /* Create a secure connection */
        *bio = BIO_new_ssl_connect(*ctx);
        if (*bio == NULL) {
            fprintf(stderr, "Error creating openssl BIO object!\n");
            ERR_print_errors_fp(stderr);
            SSL_CTX_free(*ctx);
            return 1;
        }

        BIO_set_conn_hostname(*bio, uri);
    } else {
        /* create an unsecure */
        *bio = BIO_new_connect(uri);
        if (*bio == NULL) {
            fprintf(stderr, "Error creating openssl BIO object!\n");
            ERR_print_errors_fp(stderr);
            return 1;
        }
    }

    if (BIO_do_connect(*bio) <= 0) {
        char error[65535];
        fprintf(stderr, "Failed to connect to %s\n", uri);
        ERR_error_string_n(ERR_get_error(), error, 65535);
        printf("Error: %s\n", error);
        ERR_print_errors(*bio);
        BIO_free_all(*bio);
        if (secure) {
            SSL_CTX_free(*ctx);
        }
        return 1;
    }

    /* we're connected */
    if (secure && BIO_do_handshake(*bio) <= 0) {
        fprintf(stderr, "Failed to do SSL handshake!");
        BIO_free_all(*bio);
        SSL_CTX_free(*ctx);
        return 1;
    }

    /* Do we need to do SASL auth? */
    if (user && do_sasl_auth(*bio, user, pass) == -1) {
        BIO_free_all(*bio);
        SSL_CTX_free(*ctx);
        return 1;
    }

    return 0;
}
