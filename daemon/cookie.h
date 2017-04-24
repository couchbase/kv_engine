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

#include <platform/uuid.h>
#include <stdexcept>

class Command;

class Connection;

/**
 * The Cookie class represents the cookie passed from the memcached core
 * down through the engine interface to the engine.
 *
 * With Greenstack we want to allow a connection to have multiple
 * commands being active simultaneously in the underlying engine so we can't
 * use the connection id as the identifier.
 *
 * By passing a common class as the cookie our notification model may
 * know what the argument is and provide it's own logic depending on
 * which field is set
 */
class Cookie {
public:
    Cookie(Command* cmd)
        : magic(0xdeadcafe),
          connection(nullptr),
          command(cmd) { }

    Cookie(Connection* conn)
        : magic(0xdeadcafe),
          connection(conn),
          command(nullptr) { }

    void validate() const {
        if (magic != 0xdeadcafe) {
            throw std::runtime_error(
                "Cookie::validate: Invalid magic detected");
        }
    }

    /**
     * Reset the Cookie object to allow it to be reused in the same
     * context as the last time.
     */
    void reset() {
        event_id.clear();
        error_context.clear();
        json_message.clear();
    }

    /**
     * Get the unique event identifier created for this command. It should
     * be included in all log messages related to a given request, and
     * returned in the response sent back to the client.
     *
     * @return A "random" UUID
     */
    const std::string& getEventId() const {
        if (event_id.empty()) {
            event_id = to_string(cb::uuid::random());
        }

        return event_id;
    }

    void setEventId(std::string& uuid) {
        event_id = std::move(uuid);
    }

    /**
     * Does this cookie contain a UUID to be inserted into the error
     * message to be sent back to the client.
     */
    bool hasEventId() const {
        return !event_id.empty();
    }

    /**
     * Add a more descriptive error context to response sent back for
     * this command.
     */
    void setErrorContext(std::string message) {
        error_context = std::move(message);
    }

    /**
     * Get the context to send back for this command.
     */
    const std::string& getErrorContext() const {
        return error_context;
    }

    /**
     * Return the error "object" to return to the client.
     *
     * @return An empty string if no extended error information is being set
     */
    const std::string& getErrorJson();

    /**
     * The magic byte is used for development only and will be removed when
     * we've successfully verified that we don't have any calls through the
     * engine API where we are passing invalid cookies (connection objects).
     *
     * We've always had the cookie meaning the connection object, so it could
     * be that I've missed some places where we pass something else than
     * a new cookie. We want to track those errors as soon as possible.
     */
    uint64_t magic;
    Connection* const connection;
    Command* const command;

protected:
    mutable std::string event_id;
    std::string error_context;
    /**
     * A member variable to keep the data around until it's been safely
     * transferred to the client.
     */
    std::string json_message;
};
