/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "username_util.h"
#include <stdexcept>

namespace cb::sasl::username {

std::string encode(const std::string_view username) {
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

std::string decode(const std::string_view username) {
    std::string ret(username);

    auto index = ret.find('=');
    if (index == std::string::npos) {
        return ret;
    }

    // we might want to optimize this at one point ;)
    do {
        if ((index + 3) > ret.length()) {
            throw std::runtime_error(
                    "cb::sasl::username::decode: Invalid escape"
                    " sequence, Should be =2C or =3D");
        }

        if (ret[index + 1] == '2' && ret[index + 2] == 'C') {
            ret.replace(index, 3, ",");
            index++;
        } else if (ret[index + 1] == '3' && ret[index + 2] == 'D') {
            ret.replace(index, 3, "=");
            index++;
        } else {
            throw std::runtime_error(
                    "cb::sasl::username::decode: Invalid escape"
                    " sequence. Should be =2C or =3D");
        }
    } while ((index = ret.find('=', index)) != std::string::npos);

    return ret;
}

} // namespace cb::sasl::username