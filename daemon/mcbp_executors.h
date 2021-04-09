/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

// forward decl
class Connection;
class Cookie;
namespace cb::mcbp {
class Request;
class Response;
} // namespace cb::mcbp

void initialize_mbcp_lookup_map();

void execute_request_packet(Cookie& cookie, const cb::mcbp::Request& request);

void execute_response_packet(Cookie& cookie,
                             const cb::mcbp::Response& response);
