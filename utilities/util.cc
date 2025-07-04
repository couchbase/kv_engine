/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008-2010 Danga Interactive
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>

#include <memcached/engine.h>
#include <memcached/util.h>

template <typename T>
bool parseInt(const std::string_view s, T& out) {
    const char* first = s.data();
    const char* end = s.data() + s.size();

    while (isspace(*first) && (first != end)) {
        ++first;
    }

    /* std::from_chars does not recognise the plus sign. */
    if (*first == '+') {
        ++first;
    }
    const auto [ptr, ec] = std::from_chars(first, end, out);

    if (ec != std::errc() || (ptr != end && !isspace(*ptr))) {
        out = 0;
        return false;
    }

    return true;
}

bool safe_strtoull(std::string_view s, uint64_t& out) {
    return parseInt<uint64_t>(s, out);
}

bool safe_strtoll(std::string_view s, int64_t& out) {
    return parseInt<int64_t>(s, out);
}

bool safe_strtoul(std::string_view s, uint32_t& out) {
    return parseInt<uint32_t>(s, out);
}

bool safe_strtol(std::string_view s, int32_t& out) {
    return parseInt<int32_t>(s, out);
}

bool safe_strtous(std::string_view s, uint16_t& out) {
    return parseInt<uint16_t>(s, out);
}

bool safe_strtof(const std::string& s, float& out) {
    const auto* str = s.c_str();
#ifdef WIN32
    /* Check for illegal charachters */
    const char *ptr = str;
    int space = 0;
    while (*ptr != '\0') {
        if (!isdigit(*ptr)) {
            switch (*ptr) {
            case '.':
            case ',':
            case '+':
            case '-':
                break;

            case ' ':
                ++space;
                break;
            default:
                return false;
            }
        }
        ++ptr;
        if (space) {
            break;
        }
    }


    if (ptr == str) {
        /* Empty string */
        return false;
    }
    out = (float)atof(str);
    if (errno == ERANGE) {
        return false;
    }
    return true;
#else
    char *endptr;
    float l;
    errno = 0;
    out = 0;
    l = strtof(str, &endptr);
    if (errno == ERANGE) {
        return false;
    }
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        out = l;
        return true;
    }
    return false;
#endif
}

std::string to_string(const BucketCompressionMode mode) {
    switch (mode) {
    case BucketCompressionMode::Off:
        return "off";
    case BucketCompressionMode::Passive:
        return "passive";
    case BucketCompressionMode::Active:
        return "active";
    }

    throw std::invalid_argument(
            "to_string(BucketCompressionMode): Invalid mode: " +
            std::to_string(int(mode)));
}

BucketCompressionMode parseCompressionMode(const std::string_view mode) {
    using namespace std::string_view_literals;
    if (mode == "off"sv) {
        return BucketCompressionMode::Off;
    }
    if (mode == "passive"sv) {
        return BucketCompressionMode::Passive;
    }
    if (mode == "active"sv) {
        return BucketCompressionMode::Active;
    }
    throw std::invalid_argument("setCompressionMode: invalid mode specified");
}

std::size_t parse_size_string(const std::string_view arg) {
    using namespace std::literals::string_view_literals;
    unsigned long result = 0;
    auto [next, ec] =
            std::from_chars(arg.data(), arg.data() + arg.size(), result, 10);

    if (ec == std::errc()) {
        if (next != arg.data() + arg.size()) {
            auto rest = std::string(next, arg.data() + arg.size() - next);
            for (auto& c : rest) {
                c = std::tolower(c);
            }
            if (rest == "k" || rest == "kib") {
                result *= 1_KiB;
            } else if (rest == "m" || rest == "mib") {
                result *= 1_MiB;
            } else if (rest == "g" || rest == "gib") {
                result *= 1_GiB;
            } else if (rest == "t" || rest == "tib") {
                result *= 1_TiB;
            } else if (!rest.empty()) {
                throw std::system_error(
                        std::make_error_code(std::errc::invalid_argument),
                        fmt::format(
                                "Failed to parse string. Expected: "
                                "<num>[k,m,g,t]. value:\"{}\"",
                                std::string_view(
                                        next, arg.data() + arg.size() - next)));
            }
        }
        return result;
    }

    throw std::system_error(std::make_error_code(ec),
                            fmt::format("Failed to parse string. Expected: "
                                        "<num>[k,m,g,t]. value:\"{}\"",
                                        arg));
}
