/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "json_validator_test.h"

void JsonValidatorTest::acceptString(const std::string& tag,
                                     std::vector<std::string> legalValues) {
    // Boolean values should not be accepted
    nlohmann::json json = legalSpec;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);

    // we should accept the provided values, but fail on others
    if (legalValues.empty()) {
        json[tag] = "this should be accepted";
        expectSuccess(json);
    } else {
        for (const auto& value : legalValues) {
            EXPECT_NE("this should not be accepted", value)
                    << "Can't use the text as a legal value";
            json[tag] = value;
            expectSuccess(json);
        }
        json[tag] = "this should not be accepted";
        expectFail(json);
    }
}

/// Verify that we only accept boolean values
void JsonValidatorTest::acceptBoolean(const std::string& tag) {
    // String values should not be accepted
    nlohmann::json json = legalSpec;
    json[tag] = "foo";
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);
}

/// Verify that we only allow positive integers representing an in_port_t
void JsonValidatorTest::acceptIntegers(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json = legalSpec;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // String values should not be accepted
    json[tag] = "foo";
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);

    // An object should not be accepted
    json[tag] = nlohmann::json::object();
    expectFail(json);

    json[tag] = 0.5;
    expectFail(json);
}

void JsonValidatorTest::acceptIntegers(const std::string& tag,
                                       int64_t min,
                                       int64_t max) {
    acceptIntegers(tag);

    nlohmann::json json = legalSpec;
    json[tag] = min;
    expectSuccess(json);

    json[tag] = max;
    expectSuccess(json);

    json[tag] = min - 1;
    expectFail(json);

    json[tag] = max + 1;
    expectFail(json);
}

void JsonValidatorTest::acceptObject(const std::string& tag) {
    // Boolean values should not be accepted
    nlohmann::json json = legalSpec;
    json[tag] = true;
    expectFail(json);

    json[tag] = false;
    expectFail(json);

    // Numbers should not be accepted
    json[tag] = 5;
    expectFail(json);

    json[tag] = 5.0;
    expectFail(json);

    // A string should not be accepted
    json[tag] = "foobar";
    expectFail(json);

    // An array should not be accepted
    json[tag] = nlohmann::json::array();
    expectFail(json);
}
