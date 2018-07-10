/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015 Couchbase, Inc
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

/*
 * memcached binary protocol packet validators
 */
#pragma once

#include <array>
#include <mcbp/mcbp.h>
#include <memcached/protocol_binary.h>
#include "cookie.h"
#include "function_chain.h"

/*
 * The MCBP validator chains.
 *
 * Class stores a chain per opcode allowing a sequence of command validators to be
 * configured, stored and invoked.
 *
 */
class McbpValidatorChains {
public:

    /*
     * Invoke the chain for the command
     */
    protocol_binary_response_status invoke(protocol_binary_command command,
                                           const Cookie& cookie) {
        return commandChains[command].invoke(cookie);
    }

    /*
     * Silently ignores any attempt to push the same function onto the chain.
     */
    void push_unique(protocol_binary_command command,
                     protocol_binary_response_status(*f)(const Cookie&)) {
        commandChains[command].push_unique(makeFunction<protocol_binary_response_status,
                                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                           const Cookie&>(f));
    }

    /*
     * Initialize the memcached binary protocol validators
     */
    static void initializeMcbpValidatorChains(McbpValidatorChains& chain);

private:

    std::array<FunctionChain<protocol_binary_response_status,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS,
                             const Cookie&>, 0x100> commandChains;
};

/// @return true if the keylen represents a valid key for the connection
bool is_document_key_valid(const Cookie& cookie);
