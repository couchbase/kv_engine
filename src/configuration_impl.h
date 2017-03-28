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

/*
 * Contains implementations relating to configuration.h that are not needed in
 * most places Configuration is. This can be included in a far smaller number
 * of places, reducing the overhead of including configuration.h.
 */

#pragma once

#include <boost/variant.hpp>

#include <string>
#include <vector>

using value_variant_t =
        boost::variant<size_t, ssize_t, float, bool, std::string>;

class requirements_unsatisfied : public std::logic_error {
public:
    requirements_unsatisfied(const std::string& msg) : std::logic_error(msg) {
    }
};

class Requirement {
public:
    Requirement* add(const std::string& key, value_variant_t value) {
        requirements.emplace_back(key, value);
        return this;
    }

    std::vector<std::pair<std::string, value_variant_t>> requirements;
};