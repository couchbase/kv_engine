/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * High level, generic string utility functions.
 */

#pragma once

#include <stdexcept>
#include <string>

/**
 * Converts a string to a boolean if viable, otherwise throws an exception
 *
 * Valid strings are 'true' and 'false' (Case-sensitive)
 *
 * @param s String to convert
 * @return Converted string
 * @throws std::invalid_argument_bool if argument is not true or false
 */
bool cb_stob(const std::string& s);

/**
 * Checks input to determine whether it is prefixed with 'prefix'.
 */
bool cb_isPrefix(const std::string& input,
                 const std::string& prefix);

bool cb_isPrefix(std::string_view input, const std::string& prefix);

class invalid_argument_bool : public std::invalid_argument {
public:
    explicit invalid_argument_bool(const std::string& msg)
        : std::invalid_argument(msg) {
    }
};
