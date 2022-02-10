/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "syntax_validator.h"

#include <JSON_checker.h>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace cb::json {

/// Validator using the old JSON_checker
class JSON_checkerValidator : public SyntaxValidator {
public:
    bool validate(std::string_view view) override {
        return validator.validate(view);
    }

protected:
    JSON_checker::Validator validator;
};

class NlohmannValidator : public SyntaxValidator {
public:
    bool validate(std::string_view view) override {
        return nlohmann::json::accept(view);
    }
};

SyntaxValidator::~SyntaxValidator() = default;

std::unique_ptr<SyntaxValidator> SyntaxValidator::New(Type type) {
    switch (type) {
    case Type::JSON_checker:
        return std::make_unique<JSON_checkerValidator>();
    case Type::Nlohmann:
        return std::make_unique<NlohmannValidator>();
    }
    throw std::invalid_argument("SyntaxValidator::New(): Unknown type");
}
} // namespace cb::json

std::string to_string(const cb::json::SyntaxValidator::Type& type) {
    switch (type) {
    case cb::json::SyntaxValidator::Type::JSON_checker:
        return "JSON_checker";
    case cb::json::SyntaxValidator::Type::Nlohmann:
        return "Nlohmann";
    }

    return "Unknown: " + std::to_string(int(type));
}

std::ostream& operator<<(std::ostream& os,
                         const cb::json::SyntaxValidator::Type& type) {
    os << to_string(type);
    return os;
}
