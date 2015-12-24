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
#pragma once

#include <cstdlib>
#include <memory>
#include <string>

enum class Mechanism {
    PLAIN,
    CRAM_MD5,
    UNKNOWN
};

/**
 * The base class for each supported backend.
 */
class MechanismBackend {
public:
    MechanismBackend(const std::string& nm)
        : name(nm) {

    }

    virtual ~MechanismBackend() {

    }

    /**
     * Handle the sasl_server_start method.
     *
     * @param conn the connection object requested the start
     * @param input the data sent from the client
     * @param inputlen the number of bytes received from the client
     * @param output pointer to the location to where to store the data
     *               to send back to the client
     * @param outputlen the number of bytes to send back to the client
     * @return the appropriate error code
     */
    virtual cbsasl_error_t start(cbsasl_conn_t* conn,
                                 const char* input,
                                 unsigned inputlen,
                                 const char** output,
                                 unsigned* outputlen) {
        return CBSASL_FAIL;
    }

    /**
     * Handle the sasl_server_step method.
     *
     * @param conn the connection object requested the step
     * @param input the data sent from the client
     * @param inputlen the number of bytes received from the client
     * @param output pointer to the location to where to store the data
     *               to send back to the client
     * @param outputlen the number of bytes to send back to the client
     * @return the appropriate error code
     */
    virtual cbsasl_error_t step(cbsasl_conn_t* conn,
                                const char* input,
                                unsigned inputlen,
                                const char** output,
                                unsigned* outputlen) {
        return CBSASL_FAIL;
    }

    /**
     * Get the name of the mechanism in use
     */
    const std::string& getName() const {
        return name;
    }

protected:
    /**
     * The name of the mechanism in use
     */
    std::string name;
};

typedef std::unique_ptr<MechanismBackend> UniqueMechanismBackend;

typedef int (* get_username_fn)(void* context, int id, const char** result,
                                unsigned int* len);

typedef int (* get_password_fn)(cbsasl_conn_t* conn, void* context, int id,
                                cbsasl_secret_t** psecret);


/**
 * The client API use this part of the API
 */
class ClientConnection {
public:
    /**
     * The specified callback to get the username from the client
     */
    get_username_fn get_username;
    /**
     * The context cookie for the username callback
     */
    void* get_username_ctx;

    /**
     * The specified callback to get the password from the client
     */
    get_password_fn get_password;

    /**
     * The context cookie for the password callback
     */
    void* get_password_ctx;

    /**
     * The actual backend used to implement the SASL authentication scheme
     */
    UniqueMechanismBackend mech;
};

/**
 * A server connection would use this part of the API
 */
class ServerConnection {
public:
    /**
     * The username being bound to this SASL authentication
     */
    std::string username;
    /**
     * The available mechanics for this connection (unless the default
     * string is being used)
     */
    std::string list_mechs;

    /**
    * The actual backend used to implement the SASL authentication scheme
    */
    UniqueMechanismBackend mech;
};

/**
 * This is the unified cbsasl_conn_st structure used by both clients and
 * servers. Clients should only use the client part, and servers object
 * only use the server part.
 */
struct cbsasl_conn_st {
    cbsasl_conn_st()
        : mechanism(Mechanism::PLAIN) {

    }

    /**
     * The mecanism currently selected
     */
    Mechanism mechanism;

    /**
     * The "client api" part use this member (and may ensure that it isn't
     * being incorrectly being used by the server by verifying that
     * server.get() == nullptr
     */
    std::unique_ptr<ClientConnection> client;

    /**
     * The "server api" part use this member (and may ensure that it isn't
     * being incorrectly being used by the clients by verifying that
     * client.get() == nullptr
     */
    std::unique_ptr<ServerConnection> server;
};

/**
 * A wrapper function around the get_username callback due to the fact
 * that the old API specifies int as the return type and we use
 * an enum internally. To work around that we treat any return code
 * != 0 as CBSASL_FAIL.
 */
cbsasl_error_t cbsasl_get_username(get_username_fn function,
                                   void* context,
                                   const char** username,
                                   unsigned int* usernamelen);

/**
 * A wrapper function around the get_password callback due to the fact
 * that the old API specifies int as the return type and we use
 * an enum internally. To work around that we treat any return code
 * != 0 as CBSASL_FAIL.
 */
cbsasl_error_t cbsasl_get_password(get_password_fn function,
                                   cbsasl_conn_t* conn,
                                   void* context,
                                   cbsasl_secret_t** psecret);
