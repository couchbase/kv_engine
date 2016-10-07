/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

class invalid_argument_bool : public std::invalid_argument {
public:
    invalid_argument_bool(const std::string& msg) : std::invalid_argument(msg) {
    }
};
