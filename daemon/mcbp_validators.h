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

    constexpr static uint8_t AllSupportedDatatypes =
            cb::mcbp::datatype::highest;

    McbpValidator();

    /**
     * Invoke the validator for the command if one is configured. Otherwise
     * Success is returned
     */
    Status validate(ClientOpcode command, Cookie& cookie);

    static Status verify_header(Cookie& cookie,
                                uint8_t expected_extlen,
                                ExpectedKeyLen expected_keylen,
                                ExpectedValueLen expected_valuelen,
                                ExpectedCas expected_cas,
                                uint8_t expected_datatype_mask);

protected:
    /**
     * Installs a validator for the given command
     */
    void setup(ClientOpcode command, Status (*f)(Cookie&));

    std::array<std::function<Status(Cookie&)>, 0x100> validators;
};

/**
 * Validate the key for operations which will create a DocKey
 * @param cookie non const reference as failure will update the error context
 * @return true if the keylen represents a valid key for the connection
 */
bool is_document_key_valid(Cookie& cookie);
