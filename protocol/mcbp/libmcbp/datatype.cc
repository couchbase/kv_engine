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

std::string to_string(cb::mcbp::Datatype datatype) {
    return to_string(toJSON(datatype), false);
}

unique_cJSON_ptr toJSON(cb::mcbp::Datatype datatype) {
    if (datatype == cb::mcbp::Datatype::Raw) {
        return unique_cJSON_ptr{cJSON_CreateString("raw")};
    }

    unique_cJSON_ptr ret(cJSON_CreateArray());
    auto val = uint8_t(datatype);
    if (val & uint8_t(cb::mcbp::Datatype::JSON)) {
        cJSON_AddItemToArray(ret.get(), cJSON_CreateString("JSON"));
    }
    if (val & uint8_t(cb::mcbp::Datatype::Snappy)) {
        cJSON_AddItemToArray(ret.get(), cJSON_CreateString("Snappy"));
    }
    if (val & uint8_t(cb::mcbp::Datatype::Xattr)) {
        cJSON_AddItemToArray(ret.get(), cJSON_CreateString("Xattr"));
    }

    return ret;
}
