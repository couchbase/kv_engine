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

static void parse(
        std::string_view str,
        struct config_item* items,
        std::function<void(std::string_view)> unknown_key_callback,
        std::function<void(std::string_view)> duplicate_key_callback) {
    using namespace cb::config;

    tokenize(str,
             [&items, &unknown_key_callback, &duplicate_key_callback](
                     auto key, const auto& value) {
                 int ret = 0;
                 int ii = 0;
                 while (items[ii].key) {
                     if (key == items[ii].key) {
                         if (items[ii].found) {
                             duplicate_key_callback(items[ii].key);
                         }

                         switch (items[ii].datatype) {
                         case DT_SIZE:
                             try {
                                 *items[ii].value.dt_size =
                                         value_as_size_t(value);
                                 items[ii].found = true;
                             } catch (const std::exception&) {
                                 ret = -1;
                             }
                             break;
                         case DT_SSIZE:
                             try {
                                 *items[ii].value.dt_ssize =
                                         value_as_ssize_t(value);
                                 items[ii].found = true;
                             } catch (const std::exception&) {
                                 ret = -1;
                             }
                             break;
                         case DT_FLOAT:
                             try {
                                 *items[ii].value.dt_float =
                                         value_as_float(value);
                                 items[ii].found = true;
                             } catch (const std::exception&) {
                                 ret = -1;
                             }
                             break;
                         case DT_STRING:
                             // MB-20598: free the dt_string as in case of a
                             // duplicate config entry we would leak when
                             // overwriting with the next strdup.
                             if (items[ii].found) {
                                 cb_free(*items[ii].value.dt_string);
                             }
                             *items[ii].value.dt_string =
                                     cb_strdup(value.c_str());
                             items[ii].found = true;
                             break;
                         case DT_BOOL:
                             try {
                                 *items[ii].value.dt_bool =
                                         value_as_bool(value);
                                 items[ii].found = true;
                             } catch (const std::exception&) {
                                 ret = -1;
                             }
                             break;
                         default:
                             /// You need to fix your code!!!
                             throw std::invalid_argument(fmt::format(
                                     "ERROR: Invalid datatype {} for Key: {}",
                                     int(items[ii].datatype),
                                     key));
                         }

                         if (ret == -1) {
                             throw std::runtime_error(fmt::format(
                                     "Invalid entry, Key: <{}> Value: <{}>",
                                     key,
                                     value));
                         }
                         break;
                     }
                     ++ii;
                 }

                 if (items[ii].key == nullptr) {
                     unknown_key_callback(key);
                 }
             });
}

int parse_config(const char* str, struct config_item* items, FILE* error) {
    bool unknown = false;
    auto unknown_key_callback = [&error, &unknown](std::string_view key) {
        if (error) {
            fmt::print(error, "Unsupported key: <{}>\n", key);
        }
        unknown = true;
    };

    auto duplicate_key_callback = [&error](std::string_view key) {
        if (error != nullptr) {
            fmt::print(
                    error, "WARNING: Found duplicate entry for \"{}\"\n", key);
        }
    };

    try {
        parse(str, items, unknown_key_callback, duplicate_key_callback);
    } catch (const std::exception& e) {
        if (error) {
            fprintf(error, "%s\n", e.what());
        }
        return -1;
    }
    return unknown ? 1 : 0;
}
