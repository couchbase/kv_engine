/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "config.h"
#include <libgreenstack/memcached/Datatype.h>
#include <map>
#include <stdexcept>
#include <strings.h>

const std::map<Greenstack::Datatype, std::string> mappings = {
    {Greenstack::Datatype::Raw,  "Raw"},
    {Greenstack::Datatype::Json, "JSON"}
};

std::string Greenstack::to_string(const Greenstack::Datatype& datatype) {
    const auto iter = mappings.find(datatype);
    if (iter == mappings.cend()) {
        return "Invalid/unsupported datatype: " +
               std::to_string(uint8_t(datatype));
    } else {
        return iter->second;
    }
}
