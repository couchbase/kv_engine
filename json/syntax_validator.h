/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memory>
#include <ostream>
#include <string_view>

namespace cb::json {

/// Abstract class to provide a JSON validator of a supported type
/// The motivation behind this class is to make it easy to flip the backend
/// across all components without having to update all of them.
class SyntaxValidator {
public:
    enum class Type { JSON_checker, Nlohmann };

    virtual ~SyntaxValidator();

    /// Validate that the provided view contains valid JSON
    virtual bool validate(std::string_view view) = 0;

    /// Create a new instance of the given type
    static std::unique_ptr<SyntaxValidator> New(Type = Type::JSON_checker);
};

} // namespace cb::json

std::ostream& operator<<(std::ostream& os,
                         const cb::json::SyntaxValidator::Type& t);
std::string to_string(const cb::json::SyntaxValidator::Type& type);
