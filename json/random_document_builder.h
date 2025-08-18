/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <random>

namespace cb::json {

/**
 * A class to generate random JSON documents to use in tests (in the case
 * where you just want some random data).
 *
 * The generated documents can be controlled by specifying a maximum size
 * and maximum depth. The maximum size isn't a hard limit and the returned
 * document may very well be larger than the specified size (we might not
 * account for all characters used in the JSON representation) and we might
 * add extra extra elements etc. The maximum depth is a hard limit
 *
 * NOTE: This class is not optimized for speed or for generating "realistic"
 * data. It's only purpose is to generate some random data to use in tests
 * (when used with dbfill it will cause the memcached front end threads to
 * have to parse this data; it should possibly also compress well as its
 * text so that code path would be run etc)
 */
class RandomDocumentBuilder {
public:
    RandomDocumentBuilder()
        : rng(rd()),
          double_distribution(-1000.0, 1000.0),
          int_distribution(std::numeric_limits<int>::min(),
                           std::numeric_limits<int>::max()) {
    }

    /// Generate a random JSON document
    nlohmann::json generate(std::size_t max_size, size_t max_depth = 0);

protected:
    /// The different element types we can insert into a JSON document
    enum class Type { String, Number, Float, Boolean, Object, Array };

    /// Get the next type to insert into the document
    Type nextType(size_t max_depth);

    /**
     * Get a random value (which may be an object or array if max_depth > 0)
     *
     * @param max_size The maximum size of the returned value
     * @param max_depth The maximum depth to allow within the returned value
     * @return A random JSON value of any of the allowed types
     */
    nlohmann::json value(std::size_t max_size, size_t max_depth);

    /**
     * Generate a random JSON object
     *
     * @param max_size The maximum size of the returned object
     * @param max_depth The maximum depth to allow within the returned object
     * @param elements The maximum number of elements to insert into the object
     * @return A random JSON object
     */
    nlohmann::json object(std::size_t max_size,
                          size_t max_depth,
                          std::size_t elements);

    /**
     * Generate a random JSON array
     *
     * @param max_size The maximum size of the returned array
     * @param max_depth The maximum depth to allow within the returned array
     * @param elements The maximum number of elements to insert into the array
     * @return A random JSON array
     */
    nlohmann::json array(std::size_t max_size,
                         size_t max_depth,
                         std::size_t elements);

    double getRandomDouble() {
        return double_distribution(rng);
    }

    int getRandomInt() {
        return int_distribution(rng);
    }

    std::random_device rd;
    std::mt19937 rng;
    std::uniform_real_distribution<double> double_distribution;
    std::uniform_int_distribution<int> int_distribution;
};

} // namespace cb::json
