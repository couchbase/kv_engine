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

#include <cbsasl/cbsasl.h>

#include <platform/uuid.h>

#include <cstdlib>
#include <memory>
#include <string>

using namespace cb::sasl;

enum class Mechanism {
    PLAIN,
    SCRAM_SHA1,
    SCRAM_SHA256,
    SCRAM_SHA512,
    UNKNOWN
};

/**
 * The base class for each supported backend.
 */
class MechanismBackend {
public:
    MechanismBackend(const std::string& nm, cbsasl_conn_t& connection)
        : name(nm), conn(connection) {

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
    virtual cbsasl_error_t start(const char* input,
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
    virtual cbsasl_error_t step(const char* input,
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

    /**
     * The connection this backend is bound to
     */
    cbsasl_conn_t& conn;
};

typedef std::unique_ptr<MechanismBackend> UniqueMechanismBackend;

/**
 * The client API use this part of the API
 */
class ClientConnection {
public:
    /**
     * The specified callback to get the username from the client
     */
    cbsasl_get_username_fn get_username;
    /**
     * The context cookie for the username callback
     */
    void* get_username_ctx;

    /**
     * The specified callback to get the password from the client
     */
    cbsasl_get_password_fn get_password;

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
     * The domain where the user is defined
     */
    cb::sasl::Domain domain{cb::sasl::Domain::Local};

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
        : mechanism(Mechanism::PLAIN),
          get_cnonce_fn(nullptr),
          get_cnonce_ctx(nullptr),
          getopt_fn(nullptr),
          getopt_ctx(nullptr) {

    }

    /**
     * The mecanism currently selected
     */
    Mechanism mechanism;

    /**
     * callback to get the client nonce
     */
    cbsasl_get_cnonce_fn get_cnonce_fn;

    /**
     * The context for the get nonce callback
     */
    void* get_cnonce_ctx;

    /**
     * Callback function to get options
     */
    cbsasl_getopt_fn getopt_fn;

    /**
     * context passed to getopt call
     */
    void* getopt_ctx;

    /**
     * Just a holder for the last error message string
     * created with cbsasl_strerror
     */
    std::string errormsg;

    std::string& get_uuid() {
        if (uuid.empty()) {
            uuid = to_string(cb::uuid::random());
        }

        return uuid;
    }

    /**
     * The UUID gets created on the first call to get_uuid() and is the
     * uuid to be used in all of the log messages. It is the one to
     * be returned back to the clients, so that they may map errors
     * for a given operation back to a given command.
     */
    std::string uuid;

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
cbsasl_error_t cbsasl_get_username(cbsasl_get_username_fn function,
                                   void* context,
                                   const char** username,
                                   unsigned int* usernamelen);

/**
 * A wrapper function around the get_password callback due to the fact
 * that the old API specifies int as the return type and we use
 * an enum internally. To work around that we treat any return code
 * != 0 as CBSASL_FAIL.
 */
cbsasl_error_t cbsasl_get_password(cbsasl_get_password_fn function,
                                   cbsasl_conn_t* conn,
                                   void* context,
                                   cbsasl_secret_t** psecret);

/**
 * Get the HMAC interation count to use.
 *
 * @param getopt_fn the user provided callback function
 * @param context the user provided context
 */
void cbsasl_set_hmac_iteration_count(cbsasl_getopt_fn getopt_fn, void* context);

/**
 * Set the available mechanisms
 *
 * @param getopt_fn the user provided callback function
 * @param context the user provided context
 */
void cbsasl_set_available_mechanisms(cbsasl_getopt_fn getopt_fn, void* context);

namespace cb {
namespace sasl {
namespace logging {
/**
 * Perform logging from witin the CBSASL library. The log data will
 * end up in the clients logging callback if configured.
 *
 * @param connection the connection performing the request (or nullptr
 *                   if we don't have an associated connection)
 * @param level the log level for the data
 * @param message the message to log
 */
void log(cbsasl_conn_t& connection, Level level, const std::string& message);

/**
 * Perform logging within the CBSASL library for components which isn't bound
 * to a given client.
 *
 * @param level
 * @param message
 */
void log(Level level, const std::string& message);

} // namespace logging

// To work around DLL linkage problems on windows for our unit tests
// (which statically links some of the source code) we'll implement
// the function in there..
namespace internal {
void set_scramsha_fallback_salt(const std::string& salt);

} // namespace internal

} // namespace sasl
} // namespace cb
