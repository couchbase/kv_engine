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
#include <mcbp/protocol/datatype.h>
#include <nlohmann/json.hpp>
#include <sstream>

std::string to_string(cb::mcbp::Datatype datatype) {
    return to_json(datatype).dump();
}

nlohmann::json to_json(cb::mcbp::Datatype datatype) {
    if (datatype == cb::mcbp::Datatype::Raw) {
        return "raw";
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

std::string cb::mcbp::datatype::to_string(protocol_binary_datatype_t datatype) {
    if (is_valid(datatype)) {
        if (is_raw(datatype)) {
            return std::string{"raw"};
        }
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
    return std::string{"invalid"};
}
