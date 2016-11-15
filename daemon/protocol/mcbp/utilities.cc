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
#include "utilities.h"

McbpConnection* cookie2mcbp(const void* void_cookie, const char* function) {
    const auto * cookie = reinterpret_cast<const Cookie *>(void_cookie);
    if (cookie == nullptr) {
        throw std::invalid_argument(std::string(function) +
                                    ": cookie is nullptr");
    }
    cookie->validate();
    auto* c = dynamic_cast<McbpConnection*>(cookie->connection);
    if (c == nullptr) {
        throw std::invalid_argument(std::string(function) +
                                    ": connection is nullptr");
    }
    return c;
}
