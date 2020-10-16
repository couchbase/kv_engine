/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "task.h"
#include <string>

namespace cb::mcbp {
enum class Status : uint16_t;
} // namespace cb::mcbp

/**
 * The AuthnAuthzServiceTask is an abstract class for all of the tasks
 * which use the authentication / authorization service.
 *
 * This allows for a common API for injecting messages to the external
 * authentication/authorization service.
 */
class AuthnAuthzServiceTask : public Task {
public:
    /**
     * The external auth service received a response from the provider
     *
     * @param status The status code from the auth provider
     * @param payload The payload from the auth provider (depends on the
     *                request type and the status)
     */
    virtual void externalResponse(cb::mcbp::Status status,
                                  const std::string& payload) = 0;
};
