/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <fmt/format.h>
#include <iosfwd>
#include <string>

/**
 * UserDataView technically makes tagUserData obsolete, but tagUserData
 * is used elsewhere for purposes other than logging, so have not been
 * changed.
 */

namespace cb {

/**
 * Tag user data with the surrounding userdata tags
 *
 * @param data The string to tag
 * @return A tagged string in the form: <ud>string</ud>
 */
[[nodiscard]] static std::string tagUserData(const std::string_view data) {
    return fmt::format("<ud>{}</ud>", data);
}

/**
 * Tag user data when objects of this type are printed, with surrounding
 * userdata tags. UserDataView is a non-owning type, so if ownership is required
 * use UserData
 */
class [[nodiscard]] UserDataView {
public:
    explicit UserDataView(const char* dataParam, size_t dataLen)
        : data(dataParam, dataLen){};

    explicit UserDataView(std::string_view dataParam) : data(dataParam){};

    // Retrieve tagged user data as a string
    std::string getSanitizedValue() const {
        return tagUserData(data);
    }

    // Retrieve untagged user data as a std::string_view
    std::string_view getRawValue() const {
        return data;
    }

private:
    std::string_view data;
};

std::ostream& operator<<(std::ostream& os, const cb::UserDataView& d);
inline auto format_as(const cb::UserDataView& d) {
    return d.getSanitizedValue();
}

template <typename BasicJsonType>
void to_json(BasicJsonType& j, const UserDataView& d) {
    j = format_as(d);
}

/**
 * UserData class should be used whenever sensitive user data is created
 * that could be printed in any format. UserData is an owning type, so in
 * cases where this is not needed, use UserDataView for efficiency
 */
class [[nodiscard]] UserData {
public:
    explicit UserData(std::string dataParam) : data(std::move(dataParam)){};

    explicit operator UserDataView() const {
        return UserDataView(data);
    }

    // Retrieve tagged user data as a string
    std::string getSanitizedValue() const {
        return tagUserData(data);
    }

    // Retrieve untagged data as a string
    std::string getRawValue() const {
        return data;
    }

private:
    std::string data;
};

std::ostream& operator<<(std::ostream& os, const cb::UserData& d);
inline auto format_as(const cb::UserData& d) {
    return d.getSanitizedValue();
}
template <typename BasicJsonType>
void to_json(BasicJsonType& j, const UserData& d) {
    j = format_as(d);
}
} // namespace cb
