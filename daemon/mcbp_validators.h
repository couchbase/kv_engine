/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "cookie.h"
#include "function_chain.h"
#include <mcbp/mcbp.h>
#include <memcached/protocol_binary.h>
#include <array>
#include <type_traits>

/**
 * The MCBP validator chains.
 *
 * Class stores a chain per opcode allowing a sequence of command validators
 * to be configured, stored and invoked.
 *
 */
class McbpValidatorChains {
public:
    using ClientOpcode = cb::mcbp::ClientOpcode;
    using Status = cb::mcbp::Status;

    McbpValidatorChains();

    /**
     * Invoke the chain for the command
     */
    Status invoke(ClientOpcode command, Cookie& cookie);

protected:
    /**
     * Silently ignores any attempt to push the same function onto the chain.
     */
    void push_unique(ClientOpcode command, Status (*f)(Cookie&));

    std::array<FunctionChain<Status, Status::Success, Cookie&>, 0x100>
            commandChains;
};

/// @return true if the keylen represents a valid key for the connection
bool is_document_key_valid(const Cookie& cookie);
