/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "random_document_builder.h"

#include <fmt/format.h>
#include <folly/lang/Assume.h>
#include <nlohmann/json.hpp>

namespace cb::json {

nlohmann::json RandomDocumentBuilder::generate(std::size_t max_size,
                                               size_t max_depth) {
    if ((rng() & 1) == 1) {
        return object(max_size, max_depth, std::numeric_limits<size_t>::max());
    }

    return array(max_size, max_depth, std::numeric_limits<size_t>::max());
}

nlohmann::json RandomDocumentBuilder::value(std::size_t max_size,
                                            size_t max_depth) {
    switch (nextType(max_depth)) {
    case Type::String:
        return std::string(std::min<std::size_t>(rng() % 64, max_size),
                           'a' + (rng() % 26));
    case Type::Number:
        return getRandomInt();
    case Type::Float:
        return getRandomDouble();
    case Type::Boolean:
        return rng() % 2 == 0;
    case Type::Object:
        return object(
                max_size, max_depth > 0 ? max_depth - 1 : 0, (rng() % 10) + 1);
    case Type::Array:
        return array(
                max_size, max_depth > 0 ? max_depth - 1 : 0, (rng() % 10) + 1);
    }
    folly::assume_unreachable();
}

nlohmann::json RandomDocumentBuilder::object(std::size_t max_size,
                                             size_t max_depth,
                                             std::size_t elements) {
    nlohmann::json result;

    // fill with kv-pairs
    std::size_t ii = 0;
    std::size_t remaining = max_size;
    while (remaining > 0 && elements > 0) {
        std::string key = fmt::format("key-{}", ii++);
        if (key.size() > remaining) {
            return result;
        }
        remaining -= key.size();
        nlohmann::json val = value(remaining, max_depth);
        const auto sz = val.dump().size();
        result.emplace(key, std::move(val));
        if (sz > remaining) {
            return result;
        }
        remaining -= sz;
        --elements;
    }
    return result;
}

nlohmann::json RandomDocumentBuilder::array(std::size_t max_size,
                                            size_t max_depth,
                                            std::size_t elements) {
    nlohmann::json result;

    std::size_t remaining = max_size;
    while (remaining > 0 && elements > 0) {
        nlohmann::json val = value(remaining, max_depth);
        const auto sz = val.dump().size();
        result.emplace_back(std::move(val));
        if (sz > remaining) {
            return result;
        }
        remaining -= sz;
        --elements;
    }

    return result;
}

RandomDocumentBuilder::Type RandomDocumentBuilder::nextType(size_t max_depth) {
    if (max_depth == 0) {
        std::uniform_int_distribution<int> type_dist(0, 3);
        return static_cast<Type>(type_dist(rng));
    }
    std::uniform_int_distribution<int> type_dist(0, 5);
    return static_cast<Type>(type_dist(rng));
}

} // namespace cb::json
