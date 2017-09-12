/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/collections_types.h"

#include <cctype>
#include <cstring>
#include <iostream>

namespace Collections {

uid_t makeUid(const char* uid) {
    if (std::strlen(uid) == 0 || std::strlen(uid) > 16) {
        throw std::invalid_argument(
                "Collections::convertUid uid must be > 0 and <= 16 characters: "
                "strlen(uid):" +
                std::to_string(std::strlen(uid)));
    }

    // verify that the input characters satisfy isxdigit
    for (size_t ii = 0; ii < std::strlen(uid); ii++) {
        if (uid[ii] == 0) {
            break;
        } else if (!std::isxdigit(uid[ii])) {
            throw std::invalid_argument("Collections::convertUid: uid:" +
                                        std::string(uid) + ", index:" +
                                        std::to_string(ii) + " fails isxdigit");
        }
    }

    return std::strtoull(uid, nullptr, 16);
}

std::string to_string(const Identifier& identifier) {
    return cb::to_string(identifier.getName()) + ":" +
           std::to_string(identifier.getUid());
}

std::ostream& operator<<(std::ostream& os, const Identifier& identifier) {
    os << to_string(identifier);
    return os;
}

} // end namespace Collections