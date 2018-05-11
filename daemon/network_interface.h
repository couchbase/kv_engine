/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "config.h"

#include <cJSON.h>
#include <platform/socket.h>
#include <gsl/gsl>

class NetworkInterface {
public:
    NetworkInterface() = default;
    explicit NetworkInterface(gsl::not_null<const cJSON*> json);

    std::string host;
    struct {
        std::string key;
        std::string cert;
    } ssl;
    int maxconn = 1000;
    int backlog = 1024;
    in_port_t port = 11211;
    bool ipv6 = true;
    bool ipv4 = true;
    bool tcp_nodelay = true;
    bool management = false;
};
