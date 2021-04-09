/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <platform/socket.h>

#include <string>
#include <tuple>

namespace cb::inet {

using HostSpec = std::tuple<std::string, in_port_t, sa_family_t>;

/**
 * Parse an internet address in IPv4 or IPv6 format into a
 * host, port and family.
 *
 * IPv4 address is specified as: hostname:port
 * IPv6 address is specified as: [::]:port
 *
 * @param host The string to parse;
 * @param port The port is used if none is specified in hostname
 * @returns hostname, port number and family of the resolved address
 * @throws std::runtime_exception if the port number cannot be determined
 */
HostSpec parse_hostname(const std::string& hostname,
                        const std::string& port);
} // namespace cb::inet
