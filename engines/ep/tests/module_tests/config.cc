/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "config.h"
#include <boost/algorithm/string.hpp>

config::Config::Config(std::initializer_list<Param> params) {
    for (const auto& [k, v] : params) {
        add(k, v);
    }
}

config::Config& config::Config::add(std::string_view key,
                                    const std::set<std::string>& values) {
    std::string keyString{key};
    ParamLists current;
    std::swap(current, paramLists);

    if (current.empty()) {
        current.emplace();
    }

    // Create divergent configurations each containing one of the values.
    for (auto value : values) {
        for (const auto& config : current) {
            Params newList(config);

            auto it = newList.find(keyString);
            if (it != newList.end() && it->second != value) {
                // We have the same param key with a different value
                // so we preserve that original config before we change it.
                paramLists.emplace(newList);
            }
            newList[keyString] = std::string(value);
            paramLists.emplace(std::move(newList));
        }
    }
    return *this;
}

std::vector<std::string> config::Config::toStrings() const {
    std::vector<std::string> configStrings;
    configStrings.reserve(paramLists.size());

    for (const auto& params : paramLists) {
        configStrings.push_back(createConfigurationString(params));
    }

    return configStrings;
}

config::Config::operator GTestGeneratorType() const {
    return ::testing::ValuesIn(toStrings());
}

config::Config config::Config::join(const config::Config& other) const {
    Config result;
    for (const auto& paramList : paramLists) {
        result.paramLists.insert(paramList);
    }
    for (const auto& paramList : other.paramLists) {
        result.paramLists.insert(paramList);
    }
    return result;
}

config::Config config::Config::combine(const config::Config& other) const {
    Config result(*this);
    for (const auto& params : other.paramLists) {
        for (const auto& [k, v] : params) {
            result.add(k, {v});
        }
    }
    return result;
}

std::string config::Config::createConfigurationString(
        const config::Config::Params& params) {
    std::vector<std::string> stringParams;
    stringParams.reserve(params.size());

    auto concat = [](auto&& kvp) { return kvp.first + "=" + kvp.second; };
    std::transform(params.begin(),
                   params.end(),
                   std::back_inserter(stringParams),
                   concat);

    return boost::algorithm::join(stringParams, ":");
}

config::Config config::operator|(const config::Config& lhs,
                                 const config::Config& rhs) {
    return lhs.join(rhs);
}

config::Config& config::operator|=(config::Config& lhs,
                                   const config::Config& rhs) {
    return lhs = lhs | rhs;
}

config::Config config::operator*(const config::Config& lhs,
                                 const config::Config& rhs) {
    return lhs.combine(rhs);
}

config::Config& config::operator*=(config::Config& lhs,
                                   const config::Config& rhs) {
    return lhs = lhs * rhs;
}
