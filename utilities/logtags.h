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

#include <iosfwd>
#include <string>

/**
 * UserDataView technically makes tagUserData obsolete, but tagUserData
 * is used elsewhere for purposes other than logging, so have not been
 * changed.
 */

namespace cb {
/**
 * Wrap user/customer specific data with specific tags so that these data can
 * be scrubbed away during log collection.
 */

const std::string userdataStartTag = "<ud>";
const std::string userdataEndTag = "</ud>";

/**
 * Tag user data with the surrounding userdata tags
 *
 * @param data The string to tag
 * @return A tagged string in the form: <ud>string</ud>
 */
static inline std::string tagUserData(const std::string& data) {
    return userdataStartTag + data + userdataEndTag;
}

/**
 * Tag user data when objects of this type are printed, with surrounding
 * userdata tags. UserDataView is a non-owning type, so if ownership is required
 * use UserData
 */
class UserDataView {
public:
    explicit UserDataView(const char* dataParam, size_t dataLen)
        : data(dataParam, dataLen){};

    explicit UserDataView(const uint8_t* dataParam, size_t dataLen)
        : data((const char*)dataParam, dataLen){};

    explicit UserDataView(std::string_view dataParam) : data(dataParam){};

    // Retrieve tagged user data as a string
    std::string getSanitizedValue() const {
        return userdataStartTag + std::string(data) + userdataEndTag;
    }

    // Retrieve untagged user data as a std::string_view
    std::string_view getRawValue() const {
        return data;
    }

private:
    std::string_view data;
};

std::ostream& operator<<(std::ostream& os, const cb::UserDataView& d);

/**
 * UserData class should be used whenever sensitive user data is created
 * that could be printed in any format. UserData is an owning type, so in
 * cases where this is not needed, use UserDataView for efficiency
 */
class UserData {
public:
    explicit UserData(std::string dataParam) : data(std::move(dataParam)){};

    explicit operator UserDataView() const {
        return UserDataView(data);
    }

    // Retrieve tagged user data as a string
    std::string getSanitizedValue() const {
        return userdataStartTag + data + userdataEndTag;
    }

    // Retrieve untagged data as a string
    std::string getRawValue() const {
        return data;
    }

private:
    std::string data;
};

std::ostream& operator<<(std::ostream& os, const cb::UserData& d);
} // namespace cb
