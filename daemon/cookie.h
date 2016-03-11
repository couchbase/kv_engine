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
};
