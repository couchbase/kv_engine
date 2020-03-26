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

#include "configuration.h"

#include <set>
#include <string>
#include <variant>
#include <vector>

using value_variant_t = std::variant<size_t, ssize_t, float, bool, std::string>;

std::string to_string(const value_variant_t& value);

class requirements_unsatisfied : public std::logic_error {
public:
    requirements_unsatisfied(const std::string& msg) : std::logic_error(msg) {
    }
};

/** A configuration input validator that ensures a numeric (size_t)
 * value falls between a specified upper and lower limit.
 */
class SizeRangeValidator : public ValueChangedValidator {
public:
    SizeRangeValidator() : lower(0), upper(0) {}

    SizeRangeValidator *min(size_t v) {
        lower = v;
        return this;
    }

    SizeRangeValidator *max(size_t v) {
        upper = v;
        return this;
    }

    void validateSize(const std::string& key, size_t value) override {
        if (value < lower || value > upper) {
            std::string error = "Validation Error, " + key +
                                " takes values between " +
                                std::to_string(lower) + " and " +
                                std::to_string(upper) + " (Got: " +
                                std::to_string(value) + ")";
            throw std::range_error(error);
        }
    }

    void validateSSize(const std::string& key, ssize_t value) override {
        auto s_lower = static_cast<ssize_t> (lower);
        auto s_upper = static_cast<ssize_t> (upper);

        if (value < s_lower || value > s_upper) {
            std::string error = "Validation Error, " + key +
                                " takes values between " +
                                std::to_string(s_lower) + " and " +
                                std::to_string(s_upper) + " (Got: " +
                                std::to_string(value) + ")";
            throw std::range_error(error);
        }
    }

private:
    size_t lower;
    size_t upper;
};

/**
 * A configuration input validator that ensures a signed numeric (ssize_t)
 * value falls between a specified upper and lower limit.
 */
class SSizeRangeValidator : public ValueChangedValidator {
public:
    SSizeRangeValidator() : lower(0), upper(0) {}

    SSizeRangeValidator* min(size_t v) {
        lower = v;
        return this;
    }

    SSizeRangeValidator* max(size_t v) {
        upper = v;
        return this;
    }

    void validateSSize(const std::string& key, ssize_t value) override {
        if (value < lower || value > upper) {
            std::string error = "Validation Error, " + key +
                                " takes values between " +
                                std::to_string(lower) + " and " +
                                std::to_string(upper) + " (Got: " +
                                std::to_string(value) + ")";
            throw std::range_error(error);
        }
    }

private:
    ssize_t lower;
    ssize_t upper;
};

/**
 * A configuration input validator that ensures that a numeric (float)
 * value falls between a specified upper and lower limit.
 */
class FloatRangeValidator : public ValueChangedValidator {
public:
    FloatRangeValidator() : lower(0), upper(0) {}

    FloatRangeValidator *min(float v) {
        lower = v;
        return this;
    }

    FloatRangeValidator *max(float v) {
        upper = v;
        return this;
    }

    void validateFloat(const std::string& key, float value) override {
        if (value < lower || value > upper) {
            std::string error = "Validation Error, " + key +
                                " takes values between " +
                                std::to_string(lower) + " and " +
                                std::to_string(upper) + " (Got: " +
                                std::to_string(value) + ")";
            throw std::range_error(error);
        }
    }

private:
    float lower;
    float upper;
};

/**
 * A configuration input validator that ensures that a value is one
 * from a predefined set of acceptable values.
 */
class EnumValidator : public ValueChangedValidator {
public:
    EnumValidator() {}

    EnumValidator *add(const char *s) {
        acceptable.insert(std::string(s));
        return this;
    }

    void validateString(const std::string& key, const char* value) override {
        if (acceptable.find(std::string(value)) == acceptable.end()) {
            std::string error = "Validation Error, " + key +
                                " takes one of [";
            for (const auto& it : acceptable) {
                error += it + ", ";
            }
            if (!acceptable.empty()) {
                error.pop_back();
                error.pop_back();
            }

            error += "] (Got: " + std::string(value) + ")";
            throw std::range_error(error);
        }
    }

private:
    std::set<std::string> acceptable;
};

class Requirement {
public:
    Requirement* add(const std::string& key, value_variant_t value) {
        requirements.emplace_back(key, value);
        return this;
    }

    std::vector<std::pair<std::string, value_variant_t>> requirements;
};
