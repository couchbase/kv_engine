/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <cbsasl/context.h>
#include <cbsasl/error.h>

#include <platform/sized_buffer.h>
#include <functional>
#include <memory>
#include <utility>

namespace cb {
namespace sasl {
namespace client {

class ClientContext;

/**
 * Callback function used for the SASL client to get the username to
 * authenticate as.
 */
using GetUsernameCallback = std::function<std::string()>;
/**
 * Callback function used for the SASL client to get the password to
 * authenticate with.
 */
using GetPasswordCallback = std::function<std::string()>;

/**
 * The base class for the various authentication mechanisms.
 *
 * This class is internal to the CBSASL library, but public in order
 * to allow unit tests to access it.
 */
class MechanismBackend {
public:
    explicit MechanismBackend(GetUsernameCallback user_cb,
                              GetPasswordCallback password_cb,
                              ClientContext& ctx)
        : usernameCallback(std::move(user_cb)),
          passwordCallback(std::move(password_cb)),
          context(ctx) {
    }
    virtual ~MechanismBackend() = default;
    virtual std::pair<cb::sasl::Error, cb::const_char_buffer> start() = 0;
    virtual std::pair<cb::sasl::Error, cb::const_char_buffer> step(
            cb::const_char_buffer input) = 0;
    virtual std::string getName() const = 0;

protected:
    const GetUsernameCallback usernameCallback;
    const GetPasswordCallback passwordCallback;
    ClientContext& context;
};

/**
 * ClientContext provides the client side API for SASL
 *
 * The typical implementation of the client side API looks like:
 *
 *     ClientContext client([username]()->std::string{return username;},
 *                          [password]()->std::string(return password;},
 *                          available mechanisms);
 *     auto data = client.start();
 *     if (data.first != Error::OK) {
 *        throw std::runtime_error(
 *            "Failed to start SASL authentication: " + to_string(data.first));
 *     }
 *
 *     // send data.data to server, and receive the server response
 *     if (server_response.first == Error::OK) {
 *         // authentication successful
 *         return;
 *     }
 *
 *     while (server_response.first == Error::CONTINUE) {
 *         data = client.step(server_response.second);
 *         if (data.first != Error::CONTINUE) {
 *            throw std::runtime_error(
 *                "Failed to continue SASL authentication: " +
 * to_string(data.first));
 *         }
 *         // send data to server, and receive the server response
 *     }
 *
 *     if (server_response == Error::OK) {
 *        // authentication successful
 *        return;
 *     }
 *     throw std::runtime_error(
 *         "Authentication failed: " + to_string(server.first));
 */
class ClientContext : public Context {
public:
    /**
     * Create a new instance of the ClientContext
     *
     * @param user_cb The callback method to fetch the username to
     *                use in the authentication
     * @param password_cb The callback method to fetch the password to
     *                use in the authentication
     * @param mechanisms The list of available mechanisms provided by
     *                   the server. The client will pick the most
     *                   secure method available
     * @throws cb::sasl::no_such_mechanism if none of the mechanisms
     *                   specified in the list of available mechanisms
     *                   is supported
     */
    ClientContext(GetUsernameCallback user_cb,
                  GetPasswordCallback password_cb,
                  const std::string& mechanisms);

    /**
     * Get the name of the mechanism in use by this backend.
     *
     * @return The name of the chosen authentication method
     */
    const std::string getName() const {
        return backend->getName();
    }

    /**
     * Start the authentication
     *
     * @return The challenge to send to the server
     */
    std::pair<cb::sasl::Error, cb::const_char_buffer> start() {
        return backend->start();
    }

    /**
     * Process the response received from the server and generate
     * another challenge to send to the server.
     *
     * @param input The response from the server
     * @return The challenge to send to the server
     */
    std::pair<cb::sasl::Error, cb::const_char_buffer> step(
            cb::const_char_buffer input) {
        return backend->step(input);
    }

protected:
    std::unique_ptr<MechanismBackend> backend;
};

} // namespace client
} // namespace sasl
} // namespace cb
