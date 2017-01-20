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
#include "config.h"
#include "hostname_utils.h"
#include <iostream>
#include <stdexcept>

cb::inet::HostSpec cb::inet::parse_hostname(const std::string& host,
                                            const std::string& port) {
    std::string rhost{host};
    std::string rport{port};
    sa_family_t family;

    const auto idx = rhost.find(":");
    if (idx == std::string::npos) {
        family = AF_UNSPEC;
    } else {
        // An IPv6 address may contain colon... but then it's
        // going to be more than one ...
        const auto last = rhost.rfind(":");
        if (idx == last) {
            rport = rhost.substr(idx + 1);
            rhost.resize(idx);
            family = AF_INET;
        } else {
            family = AF_INET6;
            // We have multiple ::, and it has to be enclosed with []
            // if one of them specifies a port..
            if (rhost[last - 1] == ']') {
                if (rhost[0] != '[') {
                    throw std::invalid_argument(
                        "Invalid IPv6 address specified. Should be: \"[address]:port\"");
                }

                rport = rhost.substr(last + 1);
                rhost.resize(last - 1);
                rhost = rhost.substr(1);
            }
        }
    }

    in_port_t in_port;
    bool success = false;
    try {
        std::size_t pos;
        in_port = in_port_t(std::stoi(rport, &pos));
        if (pos == rport.size()) {
            success = true;
        }
    } catch (...) {}

    if (!success) {
        // The port was specified as a numeric value.. use it
        auto* serv = getservbyname(rport.c_str(), "tcp");
        if (serv == nullptr) {
            throw std::runtime_error(
                "Failed to determine port number [" + rport + "]");
        }
        in_port = in_port_t(serv->s_port);
    }

    return HostSpec{rhost, in_port, family};
}
