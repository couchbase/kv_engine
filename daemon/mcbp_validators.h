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

#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/status.h>
#include <array>
#include <functional>

class Cookie;

/**
 * The MCBP validator.
 *
 * Class stores a validator per opcode and invokes the validator if one is
 * configured for that opcode.
 */
class McbpValidator {
public:
    using ClientOpcode = cb::mcbp::ClientOpcode;
    using Status = cb::mcbp::Status;

    enum class ExpectedKeyLen { Zero, NonZero, Any };
    enum class ExpectedValueLen { Zero, NonZero, Any };
    enum class ExpectedCas { Set, NotSet, Any };

    McbpValidator();

    /**
     * Invoke the validator for the command if one is configured. Otherwise
     * Success is returned
     */
    Status validate(ClientOpcode command, Cookie& cookie);

    static Status verify_header(
            Cookie& cookie,
            uint8_t expected_extlen,
            ExpectedKeyLen expected_keylen,
            ExpectedValueLen expected_valuelen,
            ExpectedCas expected_cas = ExpectedCas::Any,
            uint8_t expected_datatype_mask = ::mcbp::datatype::highest);

protected:
    /**
     * Installs a validator for the given command
     */
    void setup(ClientOpcode command, Status (*f)(Cookie&));

    std::array<std::function<Status(Cookie&)>, 0x100> validators;
};

/// @return true if the keylen represents a valid key for the connection
bool is_document_key_valid(const Cookie& cookie);
