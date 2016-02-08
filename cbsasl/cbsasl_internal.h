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
#include <cstdlib>
#include <memory>
#include <string>

enum class Mechanism {
    PLAIN,
    CRAM_MD5,
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

enum class cbsasl_loglevel_t : uint8_t {
    None = CBSASL_LOG_NONE, // Do not log anything.
    Error = CBSASL_LOG_ERR, // Log unusual errors. This is the default log level.
    Fail = CBSASL_LOG_FAIL, // Log all authentication failures.
    Warning = CBSASL_LOG_WARN, // Log non-fatal warnings.
    Notice = CBSASL_LOG_NOTE, // Log non-fatal warnings (more verbose than Warning).
    Debug = CBSASL_LOG_DEBUG, // Log non-fatal warnings (more verbose than Notice).
    Trace = CBSASL_LOG_TRACE, // Log traces of internal protocols.
    Password = CBSASL_LOG_PASS // Log traces of internal protocols, including passwords.
};


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
          log_fn(nullptr),
          log_ctx(nullptr),
          log_level(cbsasl_loglevel_t::Error),
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
     * The connection associated log function
     */
    cbsasl_log_fn log_fn;

    /**
     * the connection associated log context
     */
    void* log_ctx;

    /**
     * The connection associated log level
     */
    cbsasl_loglevel_t log_level;


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
 * Perform logging from witin the CBSASL library. The log data will
 * end up in the clients logging callback if configured.
 *
 * @param connection the connection performing the request (or nullptr
 *                   if we don't have an associated connection)
 * @param level the log level for the data
 * @param message the message to log
 */
void cbsasl_log(cbsasl_conn_t* connection,
                cbsasl_loglevel_t level,
                const std::string& message);

/**
 * Set the default logger functions being used if the connection don't
 * provide its own logger.
 *
 * @param log_fn the log function to call
 * @oaram context the context to pass to the log function
 */
void cbsasl_set_default_logger(cbsasl_log_fn log_fn, void* context);

/**
 * Set the log level
 *
 * @param connection the connection to update (if set to nullptr the
 *                   default loglevel)
 * @param getopt_fn the callback function specified by the user
 * @param context the specified for the callback
 */
void cbsasl_set_log_level(cbsasl_conn_t* connection,
                          cbsasl_getopt_fn getopt_fn, void* context);

/**
 * get the current logging level
 *
 * @param connection the connection object going to log (may be null)
 */
cbsasl_loglevel_t cbsasl_get_loglevel(const cbsasl_conn_t* connection);

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
