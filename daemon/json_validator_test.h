/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

class JsonValidatorTest : public ::testing::Test {
protected:
    /// all of the "accept" methods use the "spec" as the base, and override
    /// the provided key with other values. The motivation for this is that
    /// a lot of objects have "mandatory" fields which would cause the
    /// validation if they where missing
    JsonValidatorTest(nlohmann::json spec) : legalSpec(std::move(spec)) {
    }

    virtual void expectFail(const nlohmann::json& json) = 0;
    virtual void expectSuccess(const nlohmann::json& json) = 0;

    nlohmann::json legalSpec;

    /**
     * Verify that we only accept strings
     *
     * @param tag The tag to check
     * @param legalValues If provided we should only allow these strings
     */
    void acceptString(const std::string& tag,
                      std::vector<std::string> legalValues = {});

    /// Verify that we only accept boolean values
    void acceptBoolean(const std::string& tag);

    /// Verify that we only accept integers
    void acceptIntegers(const std::string& tag);

    /// Verify that we only accept integers within a given range
    void acceptIntegers(const std::string& tag, int64_t min, int64_t max);

    /// Verify that we only accept an object for tag
    void acceptObject(const std::string& tag);
};
