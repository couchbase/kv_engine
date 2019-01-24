/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include <mcbp/protocol/datatype.h>
#include <nlohmann/json.hpp>

std::string to_string(cb::mcbp::Datatype datatype) {
    return toJSON(datatype).dump();
}

nlohmann::json toJSON(cb::mcbp::Datatype datatype) {
    if (datatype == cb::mcbp::Datatype::Raw) {
        return nlohmann::json("raw");
    }

    nlohmann::json ret = nlohmann::json::array();
    auto val = uint8_t(datatype);
    if (val & uint8_t(cb::mcbp::Datatype::JSON)) {
        ret.push_back("JSON");
    }
    if (val & uint8_t(cb::mcbp::Datatype::Snappy)) {
        ret.push_back("Snappy");
    }
    if (val & uint8_t(cb::mcbp::Datatype::Xattr)) {
        ret.push_back("Xattr");
    }

    return ret;
}

std::string mcbp::datatype::to_string(protocol_binary_datatype_t datatype) {
    if (is_valid(datatype)) {
        if (is_raw(datatype)) {
            return std::string{"raw"};
        } else {
            std::stringstream ss;
            if (is_snappy(datatype)) {
                ss << "snappy,";
            }
            if (is_json(datatype)) {
                ss << "json,";
            }
            if (is_xattr(datatype)) {
                ss << "xattr,";
            }

            // remove the last ','
            std::string ret = ss.str();
            ret.resize(ret.size() - 1);
            return ret;
        }
    } else {
        return std::string{"invalid"};
    }
}
