/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */

#include <memcached/config_parser.h>

#include <fmt/core.h>
#include <platform/cb_malloc.h>
#include <cctype>
#include <cstring>

std::string cb::config::internal::next_field(std::string_view& src, char stop) {
    auto trim_tail = [](std::string& str, std::size_t minimum) {
        while (!str.empty() && str.size() > minimum && isspace(str.back())) {
            str.pop_back();
        }
        return str;
    };

    while (!src.empty() && isspace(src.front())) {
        src.remove_prefix(1);
    }

    bool escape = false;
    std::string dest;
    std::size_t minimum = 0;
    for (std::size_t ii = 0; ii < src.size(); ++ii) {
        dest.push_back(src[ii]);
        if (!escape && src[ii] == stop) {
            // Remove all input characters including the stopsign
            src.remove_prefix(ii + 1);
            dest.pop_back();
            return trim_tail(dest, minimum);
        }

        if (src[ii] == '\\' && !escape) {
            escape = true;
            minimum = dest.size();
            dest.pop_back();
        } else {
            escape = false;
        }
    }

    src = {};
    return trim_tail(dest, minimum);
}

static std::pair<size_t, std::string::size_type> get_multiplier(
        const std::string& val) {
    const char* sfx = "kmgt";
    size_t multiplier = 1;
    size_t m = 1;
    const char* p;
    std::string::size_type idx = std::string::npos;

    for (p = sfx; *p != '\0'; ++p) {
        idx = val.find_first_of(*p);
        m *= 1024;
        if (idx == std::string::npos) {
            idx = val.find_first_of(toupper(*p));
        }
        if (idx != std::string::npos) {
            multiplier = m;
            break;
        }
    }
    return {multiplier, idx};
}

size_t cb::config::value_as_size_t(const std::string& val) {
    const auto [multiplier, idx] = get_multiplier(val);
    std::size_t end = 0;
    uint64_t ret = std::stoull(val, &end) * multiplier;
    if (idx == std::string::npos) {
        if (end != val.size()) {
            throw std::runtime_error(fmt::format(
                    R"(cb::config::value_as_size_t: "{}" contains extra characters)",
                    val));
        }
    } else if (end != idx || end + 1 != val.size()) {
        throw std::runtime_error(fmt::format(
                R"(cb::config::value_as_size_t: "{}" contains extra characters)",
                val));
    }
    return ret;
}

ssize_t cb::config::value_as_ssize_t(const std::string& val) {
    const auto [multiplier, idx] = get_multiplier(val);
    std::size_t end = 0;
    int64_t ret = std::stoll(val, &end) * multiplier;
    if (idx == std::string::npos) {
        if (end != val.size()) {
            throw std::runtime_error(fmt::format(
                    R"(cb::config::value_as_ssize_t: "{}" contains extra characters)",
                    val));
        }
    } else if (end != idx || end + 1 != val.size()) {
        throw std::runtime_error(fmt::format(
                R"(cb::config::value_as_ssize_t: "{}" contains extra characters)",
                val));
    }
    return ret;
}

bool cb::config::value_as_bool(const std::string& val) {
    using namespace std::string_view_literals;
    // Fast path, check for lowercase
    if ("true"sv == val || "on"sv == val) {
        return true;
    }

    if ("false"sv == val || "off"sv == val) {
        return false;
    }

    // may be upper or a mix of case... lowercase and test again
    std::string lowercase;
    std::transform(
            val.begin(), val.end(), std::back_inserter(lowercase), tolower);

    if ("true"sv == lowercase || "on"sv == lowercase) {
        return true;
    }

    if ("false"sv == lowercase || "off"sv == lowercase) {
        return false;
    }

    throw std::runtime_error(fmt::format(
            R"(cb::config::value_as_bool: "{}" is not a boolean value)", val));
}

float cb::config::value_as_float(const std::string& val) {
    std::size_t end = 0;
    const auto ret = std::stof(val, &end);
    if (end != val.size()) {
        throw std::runtime_error(fmt::format(
                R"(cb::config::value_as_float: "{}" contains extra characters)",
                val));
    }
    return ret;
}

void cb::config::tokenize(
        std::string_view str,
        std::function<void(std::string_view, std::string)> callback) {
    if (!callback) {
        throw std::invalid_argument("parse_config: callback must be set");
    }
    while (!str.empty()) {
        auto key = internal::next_field(str, '=');
        if (key.empty()) {
            return;
        }
        auto value = internal::next_field(str, ';');
        callback(key, std::move(value));
    }
}

