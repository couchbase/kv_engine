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
#include "stringutils.h"

#include <stdexcept>
#include <cctype>

/**
 * According to the RFC:
 *
 * 2.3.  Prohibited Output
 *
 *    This profile specifies the following characters as prohibited input:
 *
 *       - Non-ASCII space characters [StringPrep, C.1.2]
 *       - ASCII control characters [StringPrep, C.2.1]
 *       - Non-ASCII control characters [StringPrep, C.2.2]
 *       - Private Use characters [StringPrep, C.3]
 *       - Non-character code points [StringPrep, C.4]
 *       - Surrogate code points [StringPrep, C.5]
 *       - Inappropriate for plain text characters [StringPrep, C.6]
 *       - Inappropriate for canonical representation characters
 *         [StringPrep, C.7]
 *       - Change display properties or deprecated characters
 *         [StringPrep, C.8]
 *       - Tagging characters [StringPrep, C.9]
 */
std::string SASLPrep(const std::string& string) {
    for (const auto& c : string) {
        if (c & 0x80) {
            throw std::runtime_error("SASLPrep: Multibyte UTF-8 is not"
                                         " implemented yet");
        }

        if (iscntrl(c)) {
            throw std::runtime_error("SASLPrep: control characters is not"
                                         " allowed");
        }
    }

    return string;
}

std::string encodeUsername(const std::string& username) {
    std::string ret(username);

    std::string::size_type index = 0;
    while ((index = ret.find_first_of(",=", index)) != std::string::npos) {
        if (ret[index] == ',') {
            ret.replace(index, 1, "=2C");
        } else {
            ret.replace(index, 1, "=3D");
        }
        ++index;
    }

    return ret;
}

std::string decodeUsername(const std::string& username) {
    std::string ret(username);

    auto index = ret.find('=');
    if (index == std::string::npos) {
        return ret;
    }

    // we might want to optimize this at one point ;)
    do {
        if ((index + 3) > ret.length()) {
            throw std::runtime_error("decodeUsername: Invalid escape"
                                         " sequence, Should be =2C or =3D");
        }

        if (ret[index + 1] == '2' && ret[index + 2] == 'C') {
            ret.replace(index, 3, ",");
            index++;
        } else if (ret[index + 1] == '3' && ret[index + 2] == 'D') {
            ret.replace(index, 3, "=");
            index++;
        } else {
            throw std::runtime_error("decodeUsername: Invalid escape"
                                         " sequence. Should be =2C or =3D");
        }
    } while ((index = ret.find('=', index)) != std::string::npos);

    return ret;
}
