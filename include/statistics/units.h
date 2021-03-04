/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/lang/Assume.h>
#include <gsl/gsl>

#include <array>
#include <ratio>
#include <string>
#include <string_view>

namespace cb::stats {

// Enum of the relevant base units used by convention in Prometheus
enum class BaseUnit {
    None, // used for text stats where units aren't relevant
    Count, // Generic count which does not meet a better unit
    Seconds,
    Bytes,
    Ratio,
};

/**
 * Type representing a unit (kilobytes, microseconds etc.).
 * This is encoded as a BaseUnit (see above) and a scaling factor stored as
 * two integers (see std::ratio).
 *
 * Thus,
 *     kilobytes -> bytes, 1000/1
 *     microseconds -> seconds, 1/1000000
 *
 * Given a numerical value in this unit, the scaling factor indicates
 * how to convert the value back to the base unit.
 *
 *     10 kilobytes
 *     10 * 1000 / 1
 *     -> 10000 bytes
 *
 * This can be performed using the toBaseUnit method -
 *
 * Unit(std::kilo{}, BaseUnit::Bytes).toBaseUnit(10) == 10000
 *
 * Units will usually be used through the constexpr values
 * in namespace units
 *
 * units::kilobytes.toBaseUnit(10) == 10000
 */
class Unit {
public:
    explicit constexpr Unit(BaseUnit baseUnit)
        : Unit(std::ratio<1>{}, baseUnit) {
    }

    template <class RatioType>
    constexpr Unit(RatioType ratio, BaseUnit baseUnit)
        : numerator(gsl::narrow_cast<int64_t>(RatioType::num)),
          denominator(gsl::narrow_cast<int64_t>(RatioType::den)),
          baseUnit(baseUnit) {
    }

    /**
     * Scale a value of the current unit (e.g., milliseconds) to the base
     * unit (e.g., seconds).
     */
    [[nodiscard]] constexpr double toBaseUnit(double value) const {
        return (value * numerator) / denominator;
    }

    [[nodiscard]] std::string_view getSuffix() const {
        using namespace std::string_view_literals;
        switch (baseUnit) {
        case BaseUnit::None:
        case BaseUnit::Count:
            return ""sv;
        case BaseUnit::Seconds:
            return "_seconds"sv;
        case BaseUnit::Bytes:
            return "_bytes"sv;
        case BaseUnit::Ratio:
            return "_ratio"sv;
        }
        folly::assume_unreachable();
    }

private:
    int64_t numerator;
    int64_t denominator;
    BaseUnit baseUnit;
};

namespace units {
constexpr Unit none{std::ratio<1>{}, BaseUnit::None};

constexpr Unit count{std::ratio<1>{}, BaseUnit::Count};

// floating point between 0 and 1, this is already in the correct base unit
constexpr Unit ratio{std::ratio<1>{}, BaseUnit::Ratio};
// percents should be scaled down to ratios for Prometheus
constexpr Unit percent{std::ratio<1, 100>{}, BaseUnit::Ratio};

// time units
constexpr Unit nanoseconds{std::nano{}, BaseUnit::Seconds};
constexpr Unit microseconds{std::micro{}, BaseUnit::Seconds};
constexpr Unit milliseconds{std::milli{}, BaseUnit::Seconds};
constexpr Unit seconds{std::ratio<1>{}, BaseUnit::Seconds};
constexpr Unit minutes{std::ratio<60>{}, BaseUnit::Seconds};
constexpr Unit hours{std::ratio<60 * 60>{}, BaseUnit::Seconds};
constexpr Unit days{std::ratio<60 * 60 * 24>{}, BaseUnit::Seconds};

// Note: std ratio definitions are powers of 10, not 2
// e.g., kilo = std::ratio<1000, 1>
// so 1 kilobyte = 1000 bytes
// if powers of 2 are explicitly needed, IEC prefixes
// (kibi, gibi, tebi) could easily be defined.

// byte units
constexpr Unit bits{std::ratio<1, 8>{}, BaseUnit::Bytes};
constexpr Unit bytes{std::ratio<1>{}, BaseUnit::Bytes};
constexpr Unit kilobytes{std::kilo{}, BaseUnit::Bytes};
constexpr Unit megabytes{std::mega{}, BaseUnit::Bytes};
constexpr Unit gigabytes{std::giga{}, BaseUnit::Bytes};
} // namespace units
} // namespace cb::stats