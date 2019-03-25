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

// forward decl
class Connection;
class Cookie;
namespace cb {
namespace mcbp {
class Request;
class Response;
} // namespace mcbp
} // namespace cb

void initialize_mbcp_lookup_map();

void execute_request_packet(Cookie& cookie, const cb::mcbp::Request& request);

void execute_response_packet(Cookie& cookie,
                             const cb::mcbp::Response& response);
