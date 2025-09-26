/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "configuration.h"

#include "configuration_impl.h"
#include "ep_parameters.h"

#include <folly/ScopeGuard.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <memory>

using namespace std::string_view_literals;

/* Like EXPECT_THROW except you can check the exception's `what()` */
# define CB_EXPECT_THROW_MSG(EXPR, ETYPE, MSG) \
    EXPECT_THROW(EXPR, ETYPE); \
    try { \
        EXPR; \
        ADD_FAILURE() << "Expected: " #EXPR " throws an exception of type " \
          #ETYPE ".\n  Actual: it throws nothing."; \
    } catch (const ETYPE& EXCEPTION) { \
        EXPECT_STREQ(MSG, EXCEPTION.what()) << "Wrong exception message!" ; \
    } catch (...) { \
        ADD_FAILURE() << "Expected: " #EXPR " throws an exception of type " \
          #ETYPE ".\n  Actual: it throws different type."; \
    } \

TEST(ValueChangedValidatorTest, AllMethodsThrow) {
    ValueChangedValidator validator;
    std::string key{"test_key"};

    EXPECT_THROW(validator.validateBool(key, false), std::runtime_error);
    EXPECT_THROW(validator.validateSize(key, 0), std::runtime_error);
    EXPECT_THROW(validator.validateSSize(key, 0), std::runtime_error);
    EXPECT_THROW(validator.validateFloat(key, 0.0), std::runtime_error);
    EXPECT_THROW(validator.validateString(key, ""), std::runtime_error);
}

TEST(SizeRangeValidatorTest, UninitialisedIsOnlyZero) {
    SizeRangeValidator validator;
    std::string key{"test_key"};

    EXPECT_NO_THROW(validator.validateSize(key, 0));
    EXPECT_NO_THROW(validator.validateSSize(key, 0));

    CB_EXPECT_THROW_MSG(
            validator.validateSize(key, 1),
            std::range_error,
            "Validation Error, test_key takes values between 0 and 0 (Got: 1)"
    );
    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, 1),
            std::range_error,
            "Validation Error, test_key takes values between 0 and 0 (Got: 1)"
    );
    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, -1),
            std::range_error,
            "Validation Error, test_key takes values between 0 and 0 (Got: -1)"
    );
}

TEST(SizeRangeValidatorTest, UnsignedBoundsWorks) {
    SizeRangeValidator validator;
    std::string key{"test_key"};

    (&validator)->min(100)->max(1000);

    EXPECT_NO_THROW(validator.validateSize(key, 100));
    EXPECT_NO_THROW(validator.validateSize(key, 1000));
    EXPECT_NO_THROW(validator.validateSize(key, 101));
    EXPECT_NO_THROW(validator.validateSize(key, 999));

    CB_EXPECT_THROW_MSG(
            validator.validateSize(key, 99),
            std::range_error,
            "Validation Error, test_key takes values between 100 and 1000 (Got: 99)"
    );
    CB_EXPECT_THROW_MSG(
            validator.validateSize(key, 1001),
            std::range_error,
            "Validation Error, test_key takes values between 100 and 1000 (Got: 1001)"
    );
}

TEST(SizeRangeValidatorTest, SignedBoundsWorks) {
    SSizeRangeValidator validator;
    std::string key{"test_key"};

    (&validator)->min(-100)->max(1000);

    EXPECT_NO_THROW(validator.validateSSize(key, -100));
    EXPECT_NO_THROW(validator.validateSSize(key, 1000));
    EXPECT_NO_THROW(validator.validateSSize(key, -99));
    EXPECT_NO_THROW(validator.validateSSize(key, 999));

    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, -101),
            std::range_error,
            "Validation Error, test_key takes values between -100 and 1000 (Got: -101)"
    );
    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, 1001),
            std::range_error,
            "Validation Error, test_key takes values between -100 and 1000 (Got: 1001)"
    );
}

TEST(SSizeRangeValidatorTest, UninitialisedIsOnlyZero) {
    SizeRangeValidator validator;
    std::string key{"test_key"};

    EXPECT_NO_THROW(validator.validateSSize(key, 0));

    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, 1),
            std::range_error,
            "Validation Error, test_key takes values between 0 and 0 (Got: 1)"
    );
    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, -1),
            std::range_error,
            "Validation Error, test_key takes values between 0 and 0 (Got: -1)"
    );
}

TEST(SSizeRangeValidatorTest, SignedBoundsWork) {
    SSizeRangeValidator validator;
    std::string key{"test_key"};

    (&validator)->min(-100)->max(1000);

    EXPECT_NO_THROW(validator.validateSSize(key, -100));
    EXPECT_NO_THROW(validator.validateSSize(key, 1000));
    EXPECT_NO_THROW(validator.validateSSize(key, -99));
    EXPECT_NO_THROW(validator.validateSSize(key, 999));

    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, -101),
            std::range_error,
            "Validation Error, test_key takes values between -100 and 1000 (Got: -101)"
    );
    CB_EXPECT_THROW_MSG(
            validator.validateSSize(key, 1001),
            std::range_error,
            "Validation Error, test_key takes values between -100 and 1000 (Got: 1001)"
    );
}

TEST(FloatRangeValidatorTest, UninitialisedIsZero) {
    FloatRangeValidator validator;
    std::string key{"test_key"};

    validator.validateFloat(key, 0.0);
    CB_EXPECT_THROW_MSG(
            validator.validateFloat(key, 1.0),
            std::range_error,
            "Validation Error, test_key takes values between 0.000000 and 0.000000 (Got: 1.000000)"
    );

}

TEST(FloatRangeValidatorTest, FloatBoundsWork) {
    FloatRangeValidator validator;
    std::string key{"test_key"};

    (&validator)->min(-100.1f)->max(1000.1f);

    /* In-bounds */
    EXPECT_NO_THROW(validator.validateFloat(key, -100.1f));
    EXPECT_NO_THROW(validator.validateFloat(key, 100.00f));
    EXPECT_NO_THROW(validator.validateFloat(key, 101.0f));
    EXPECT_NO_THROW(validator.validateFloat(key, 999.0f));

    /* Out-of bounds */
    CB_EXPECT_THROW_MSG(
            validator.validateFloat(key, -100.2f),
            std::range_error,
            ("Validation Error, test_key takes values between " +
             std::to_string(-100.1f) + " and " + std::to_string(1000.1f) +
             " (Got: " + std::to_string(-100.2f) + ")")
                    .c_str());
    CB_EXPECT_THROW_MSG(
            validator.validateFloat(key, 1000.2f),
            std::range_error,
            ("Validation Error, test_key takes values between " +
             std::to_string(-100.1f) + " and " + std::to_string(1000.1f) +
             " (Got: " + std::to_string(1000.2f) + ")")
                    .c_str());
}

TEST(EnumValidatorTest, EmptyWorks) {
    EnumValidator validator;
    std::string key{"test_key"};

    /* Empty test */
    CB_EXPECT_THROW_MSG(
            validator.validateString(key, "my_enum"),
            std::range_error,
            "Validation Error, test_key takes one of [] (Got: my_enum)"
    );
}

TEST(EnumValidatorTest, AddOneWorks) {
    EnumValidator validator;
    std::string key{"test_key"};

    /* Single test, in-bounds */
    validator.add("enum_one");
    EXPECT_NO_THROW(validator.validateString(key, "enum_one"));

    /* Single test, out of bounds */
    CB_EXPECT_THROW_MSG(
            validator.validateString(key, "my_enum"),
            std::range_error,
            "Validation Error, test_key takes one of [enum_one] (Got: my_enum)"
    );
}

TEST(EnumValidatorTest, OverwriteWorks) {
    EnumValidator validator;
    std::string key{"test_key"};

    validator.add("enum_one");
    validator.add("enum_one");
    EXPECT_NO_THROW(validator.validateString(key, "enum_one"));
}

TEST(EnumValidatorTest, MultipleWorks) {
    EnumValidator validator;
    std::string key{"test_key"};

    validator.add("enum_1");
    validator.add("enum_2");
    validator.add("enum_3");
    EXPECT_NO_THROW(validator.validateString(key, "enum_1"));
    EXPECT_NO_THROW(validator.validateString(key, "enum_2"));
    EXPECT_NO_THROW(validator.validateString(key, "enum_3"));

    /* Multi, out of bounds */
    CB_EXPECT_THROW_MSG(
            validator.validateString(key, "my_enum"),
            std::range_error,
            "Validation Error, test_key takes one of [enum_1, enum_2, enum_3] (Got: my_enum)"
    );
}

TEST(EnumValidatorTest, EnMassWorks) {
    EnumValidator validator;
    std::string key{"test_key"};

    /* Mass test */
    std::vector<std::string> mass;
    for(auto i = 0; i < 100000; i++) {
        mass.push_back(std::to_string(i));
    }

    for(auto i : mass) {
        validator.add(i.c_str());
    }
    for(auto i : mass) {
        EXPECT_NO_THROW(validator.validateString(key, i.c_str()));
    }
}


class ConfigurationShim : public Configuration {
    /**
     * Shim class to allow testing the protected methods which are usually
     * exposed through the generated configuration.
     */

public:
    using Configuration::Configuration;
    using Configuration::getParameter;
    using Configuration::setParameter;
    using Configuration::setParametersInternal;
    using Configuration::setRequirements;

    void public_clear() {
        attributes.clear();
    }

    template <class T>
    void public_addParameter(std::string_view key, T value, bool dynamic) {
        public_addParameter(key, value, {}, {}, {}, dynamic);
    }

    template <class T>
    void public_addParameter(std::string_view key,
                             T defaultVal,
                             std::optional<T> defaultServerless,
                             std::optional<T> defaultTSAN,
                             std::optional<T> defaultDevAssert,
                             bool dynamic) {
        initialized = false;
        Configuration::addParameter(key,
                                    defaultVal,
                                    defaultServerless,
                                    defaultTSAN,
                                    defaultDevAssert,
                                    dynamic);
        initialized = true;
    }

    void public_setRequirements(const std::string& key,
                                Requirement* requirement) {
        initialized = false;
        Configuration::setRequirements(key, requirement);
        initialized = true;
    }

    /**
     * For testing, we cannot use the validateParameters method directly,
     * because it validates against the default set of parameters, initialized
     * by the Configuration constructor.
     *
     * Instead, we use this method, which sets the parameters against this
     * object.
     */
    ParameterValidationMap setAndValidate(const ParameterMap& parameters) {
        auto [map, success] = setParametersInternal(parameters);
        fillDefaults(map);
        return map;
    }

    ValueChangedValidator* public_setValueValidator(
            std::string_view key, ValueChangedValidator* validator) {
        initialized = false;
        auto* result = Configuration::setValueValidator(key, validator);
        initialized = true;
        return result;
    }
};

TEST(ConfigurationTest, SetGetWorks) {
    ConfigurationShim configuration;

    configuration.public_addParameter("bool", false, false);
    EXPECT_EQ(configuration.getParameter<bool>("bool"), false);

    configuration.public_addParameter("size", (size_t)100, false);
    EXPECT_EQ(configuration.getParameter<size_t>("size"), 100);

    configuration.public_addParameter("ssize", (ssize_t)-100, false);
    EXPECT_EQ(configuration.getParameter<ssize_t>("ssize"), -100);

    configuration.public_addParameter("float", (float)123.5, false);
    EXPECT_EQ(configuration.getParameter<float>("float"), 123.5);

    configuration.public_addParameter("string", std::string("hello"), false);
    EXPECT_EQ(configuration.getParameter<std::string>("string"), "hello");
}

TEST(ConfigurationTest, ValidatorWorks) {
    ConfigurationShim configuration;
    std::string key{"test_key"};

    configuration.public_addParameter(key, (size_t)110, false);
    EXPECT_NO_THROW(configuration.public_setValueValidator(
            key, (new SizeRangeValidator())->min(10)->max(100)));
    EXPECT_NO_THROW(configuration.setParameter(key, (size_t)10));
    EXPECT_NO_THROW(configuration.setParameter(key, (size_t)100));
    EXPECT_THROW(configuration.setParameter(key, (size_t)9), std::range_error);

    CB_EXPECT_THROW_MSG(configuration.setParameter(key, (size_t)9),
                        std::range_error,
                        "Validation Error, test_key takes values between 10 "
                        "and 100 (Got: 9)");
}

TEST(ConfigurationTest, GeneratedEnumWorks) {
    using namespace cb::config;
    ConfigurationShim configuration;

    // Test using arbitrary dynamic enum parameter.
    for (auto mode : {DcpOsoBackfill::Auto, DcpOsoBackfill::Enabled}) {
        // Set using string.
        EXPECT_NO_THROW(configuration.setDcpOsoBackfill(to_string(mode)))
                << "Setting mode to " << format_as(mode);
        // Set using enum.
        EXPECT_NO_THROW(configuration.setDcpOsoBackfill(mode))
                << "Setting mode to " << format_as(mode);
        // Read back as string.
        EXPECT_EQ(to_string(mode), configuration.getDcpOsoBackfillString());
        // Read back as enum.
        EXPECT_EQ(mode, configuration.getDcpOsoBackfill());
    }

    // Ensure validation works.
    DcpOsoBackfill mode;
    EXPECT_THROW(from_string(mode, "nonexistent"), std::range_error);
    EXPECT_THROW(configuration.setDcpOsoBackfill("nonexistent"),
                 std::range_error);
    EXPECT_THROW(configuration.setParameter("dcp_oso_backfill", "nonexistent"),
                 std::range_error);
}

class MockValueChangedListener : public ValueChangedListener {
public:
    MOCK_METHOD2(booleanValueChanged, void(std::string_view, bool));
    MOCK_METHOD2(sizeValueChanged, void(std::string_view, size_t));
    MOCK_METHOD2(ssizeValueChanged, void(std::string_view, ssize_t));
    MOCK_METHOD2(floatValueChanged, void(std::string_view, float));
    MOCK_METHOD2(stringValueChanged, void(std::string_view, const char*));
};

using ::testing::_;

TEST(ChangeListenerTest, ChangeListenerSSizeRegression) {
    /*
     * Confirming that setting a config parameter of type ssize_t
     * correctly calls ssizeValueChanged on Changelisteners. Previously, it
     * instead called sizeValueChanged - and some code had become dependent on
     * this.
     */
    ConfigurationShim configuration;
    std::string key{"test_key"};

    // Create listeners
    auto mvcl = std::make_unique<MockValueChangedListener>();
    // set parameter once so entry in attributes is present to add a listener
    configuration.public_addParameter(key, (ssize_t)1, false);

    EXPECT_CALL(*mvcl, ssizeValueChanged("test_key", 2)).Times(1);
    EXPECT_CALL(*mvcl, sizeValueChanged(_, _)).Times(0);

    // add listeners
    configuration.addValueChangedListener(key, std::move(mvcl));

    // change parameters
    configuration.setParameter(key, (ssize_t)2);
}

TEST(ChangeListenerTest, Callback) {
    // Check that callbacks added as a lambda are called as expected.
    ConfigurationShim configuration;
    std::string key{"test_key"};

    bool testValue = false;

    configuration.public_addParameter(
            key, false /* value */, true /* isDynamic */);

    configuration.addAndNotifyValueChangedCallback(
            key, [&](bool value) { testValue = value; });

    ASSERT_FALSE(testValue);
    configuration.setParameter(key, true);
    EXPECT_TRUE(testValue);
    configuration.setParameter(key, false);
    EXPECT_FALSE(testValue);
}

TEST(ChangeListenerTest, CallbackIncorrectType) {
    // Check that callbacks added as a lambda are required to accept the
    // correct type - e.g., adding a callback accepting a string value
    // for a config param of type size_t should not be permitted
    ConfigurationShim configuration;
    std::string key{"test_key"};

    // create a config param with value of type size_t
    configuration.public_addParameter(
            key, size_t() /* value */, true /* isDynamic */);

    EXPECT_THROW(configuration.addAndNotifyValueChangedCallback(
                         key, [](std::string_view value) {}),
                 std::logic_error);

    EXPECT_NO_THROW(configuration.addAndNotifyValueChangedCallback(
            key, [](size_t value) {}));
}

TEST(ChangeListenerTest, CallbackProvidedCurrentValue) {
    // Check that callbacks added as a lambda are immediately invoked
    // with the existing value of the config param
    ConfigurationShim configuration;
    std::string key{"test_key"};

    // create a config param with value of type size_t
    configuration.public_addParameter(
            key, size_t(1234) /* value */, true /* isDynamic */);

    using namespace testing;
    StrictMock<MockFunction<void(size_t)>> mockCB;

    EXPECT_CALL(mockCB, Call(1234)).Times(1);
    configuration.addAndNotifyValueChangedCallback(key, mockCB.AsStdFunction());
}

TEST(ChangeListenerTest, CallbackProvidedCurrentValueNonDefault) {
    // As with CallbackProvidedDefaultValue, but verify that it is definitely
    // called with the _current_ value, not the _default_ value
    ConfigurationShim configuration;
    std::string key{"test_key"};

    // create a config param with value of type size_t
    configuration.public_addParameter(
            key, size_t(1234) /* value */, true /* isDynamic */);

    configuration.setParameter(key, size_t(4321));

    using namespace testing;
    StrictMock<MockFunction<void(size_t)>> mockCB;

    EXPECT_CALL(mockCB, Call(4321)).Times(1);
    configuration.addAndNotifyValueChangedCallback(key, mockCB.AsStdFunction());
}

TEST(ConfigurationTest, TsanOverride) {
    for (bool serverless : {false, true}) {
        ConfigurationShim config(serverless);
        config.public_addParameter("param",
                                   size_t(3),
                                   {size_t(2)},
                                   {size_t(1)},
                                   {size_t(0)},
                                   false);
        auto value = config.getParameter<size_t>("param");
        if (folly::kIsSanitizeThread) {
            EXPECT_EQ(1, value);
        } else {
            EXPECT_EQ((serverless ? 2 : 3), value);
        }
    }
}

TEST(ConfigurationTest, DevAssertOverride) {
    for (bool devAssertEnabled : {false, true}) {
        ConfigurationShim config(false /* serverless */, devAssertEnabled);
        config.public_addParameter("param",
                                   size_t(3),
                                   {size_t(2)},
                                   {size_t(1)},
                                   {size_t(0)},
                                   false);
        auto value = config.getParameter<size_t>("param");
        if (folly::kIsSanitizeThread) {
            EXPECT_EQ(1, value);
        } else {
            EXPECT_EQ(devAssertEnabled ? 0 : 3, value);
        }
    }
}

TEST(ConfigurationTest, conditionals) {
    ConfigurationShim configuration1, configuration2;
    configuration1.parseConfiguration("bucket_type=ephemeral");
    configuration2.parseConfiguration("bucket_type=persistent");
    if (folly::kIsSanitizeThread) {
        // ht_locks is reduced for TSAN
        ASSERT_EQ(23, configuration1.getHtLocks());
    }

    ASSERT_FALSE(configuration1.isDcpBackfillIdleProtectionEnabled());
    ASSERT_TRUE(configuration2.isDcpBackfillIdleProtectionEnabled());
}

TEST(ConfigurationTest, DynamicParametersAreSettable) {
    auto parameters = Configuration::getDynamicParametersForTesting();
    auto settableParameters = getSetParameterKeys();

    EXPECT_EQ(parameters.size(), settableParameters.size());
    for (const auto& parameter : parameters) {
        EXPECT_TRUE(settableParameters.contains(parameter))
                << "Parameter " << parameter
                << " is dynamic in the configuration but is not listed as "
                   "settable";
    }
    for (const auto& parameter : settableParameters) {
        EXPECT_TRUE(parameters.contains(std::string(parameter)))
                << "Parameter " << parameter
                << " is listed as settable but is not dynamic in the "
                   "configuration";
    }
}

TEST(ConfigurationTest, ValidateSuccess) {
    ConfigurationShim configuration;
    ParameterMap params = {{"bucket_type", "ephemeral"}};
    auto validation = nlohmann::json(
            configuration.validateParameters(params));
    auto exitGuard = folly::makeGuard([validation]() {
        if (HasFailure()) {
            FAIL() << validation.dump(4);
        }
    });

    EXPECT_GT(validation.size(), 10);
    EXPECT_EQ(validation["bucket_type"]["value"], "ephemeral");
}

TEST(ConfigurationTest, ValidateInvalidValue) {
    ConfigurationShim configuration;
    auto validation = nlohmann::json(configuration.validateParameters(
            {{"item_eviction_policy", "invalid"}}));
    auto exitGuard = folly::makeGuard([validation]() {
        if (HasFailure()) {
            FAIL() << validation.dump(4);
        }
    });

    EXPECT_EQ(validation.size(), 1);
    EXPECT_EQ(validation["item_eviction_policy"]["error"],
              "invalid_arguments");
    EXPECT_EQ(validation["item_eviction_policy"]["message"],
              "Invalid value for item_eviction_policy: invalid");
}

TEST(ConfigurationTest, ValidateNonExistent) {
    ConfigurationShim configuration;
    auto validation = nlohmann::json(
            configuration.validateParameters({{"non_existent", "1234"}}));
    auto exitGuard = folly::makeGuard([validation]() {
        if (HasFailure()) {
            FAIL() << validation.dump(4);
        }
    });

    EXPECT_GT(validation.size(), 10);
    EXPECT_EQ(validation["non_existent"]["error"], "unsupported");
    EXPECT_EQ(validation["non_existent"]["message"],
              "Parameter not supported by this bucket");
}

TEST(ConfigurationTest, ValidateDefaults) {
    ConfigurationShim configuration;
    auto validation = nlohmann::json(configuration.validateParameters({}));
    auto exitGuard = folly::makeGuard([validation]() {
        if (HasFailure()) {
            FAIL() << validation.dump(4);
        }
    });

    EXPECT_GT(validation.size(), 10);
}

TEST(ConfigurationTest, ValidateOutOfRange) {
    ConfigurationShim configuration;
    auto validation = nlohmann::json(
            configuration.validateParameters({{"mutation_mem_ratio", "1000"}}));
    auto exitGuard = folly::makeGuard([validation]() {
        if (HasFailure()) {
            FAIL() << validation.dump(4);
        }
    });

    EXPECT_EQ(validation["mutation_mem_ratio"]["error"], "invalid_arguments");
    EXPECT_TRUE(validation["mutation_mem_ratio"]["message"]
                        .get<std::string>()
                        .contains("Validation Error"))
            << validation.dump(4);
}

TEST(ConfigurationTest, ValidateInvalidType) {
    ConfigurationShim configuration;
    auto validation = nlohmann::json(configuration.validateParameters(
            {{"access_scanner_enabled", "bool"},
             {"alog_sleep_time", "int"},
             {"checkpoint_memory_ratio", "double"}}));
    auto exitGuard = folly::makeGuard([validation]() {
        if (HasFailure()) {
            FAIL() << validation.dump(4);
        }
    });

    EXPECT_EQ(validation["access_scanner_enabled"]["error"],
              "invalid_arguments");
    EXPECT_EQ(validation["alog_sleep_time"]["error"], "invalid_arguments");
    EXPECT_EQ(validation["checkpoint_memory_ratio"]["error"],
              "invalid_arguments");
}

TEST(ConfigurationTest, ValidateModifiedDefaultsReturned) {
    ConfigurationShim configuration;
    configuration.setParameter("max_item_size", size_t(1234));
    auto validation = nlohmann::json(configuration.setAndValidate({}));
    EXPECT_EQ(validation["max_item_size"]["value"], 1234);
}

static ConfigurationShim createTestConfiguration() {
    ConfigurationShim configuration;
    configuration.public_clear();
    configuration.public_addParameter(
            "bucket_type", (std::string) "persistent", false);
    configuration.public_addParameter(
            "backend", (std::string) "couchdb", false);
    configuration.public_setRequirements(
            "backend",
            (new Requirement)->add("bucket_type", (std::string) "persistent"));
    return configuration;
}

// "backend" depends on "bucket_type" being "persistent".
// Check that when "bucket_type" is "not_persistent", "backend" does not
// exist in the validation result.
TEST(ConfigurationTest, ValidateWithRequirementsMet) {
    auto validation = nlohmann::json(createTestConfiguration().setAndValidate(
            {{"bucket_type", "not_persistent"}}));
    EXPECT_EQ(validation.size(), 1) << validation.dump(4);
    EXPECT_TRUE(validation["bucket_type"]["error"].is_null())
            << validation.dump(4);
}

// Check that when "bucket_type" is "persistent", "backend" exists in the
// validation result.
TEST(ConfigurationTest, ValidateWithRequirementsHidesParameter) {
    auto validation = nlohmann::json(createTestConfiguration().setAndValidate(
            {{"bucket_type", "persistent"}}));
    EXPECT_EQ(validation.size(), 2) << validation.dump(4);
    EXPECT_TRUE(validation["bucket_type"]["error"].is_null())
            << validation.dump(4);
}

// Check that when "bucket_type" is "not_persistent", "backend" is not
// allowed to be set.
TEST(ConfigurationTest, ValidateWithRequirementsNotMet) {
    auto validation = nlohmann::json(createTestConfiguration().setAndValidate(
            {{"bucket_type", "not_persistent"}, {"backend", "couchdb"}}));
    EXPECT_EQ(validation.size(), 2) << validation.dump(4);
    EXPECT_EQ(validation["backend"]["error"], "invalid_arguments")
            << validation.dump(4);
}

TEST(ConfigurationTest, ValidateDynamic) {
    ConfigurationShim configuration;
    configuration.public_clear();
    configuration.public_addParameter("param", size_t(1), true);
    auto validation = nlohmann::json(configuration.setAndValidate({}));
    EXPECT_EQ(validation.size(), 1) << validation.dump(4);
    EXPECT_EQ(validation["param"]["value"], 1);
    EXPECT_EQ(validation["param"]["requiresRestart"], false);
}

TEST(ConfigurationTest, ValidateNonDynamic) {
    ConfigurationShim configuration;
    configuration.public_clear();
    configuration.public_addParameter("param", size_t(1), false);
    auto validation = nlohmann::json(configuration.setAndValidate({}));
    EXPECT_EQ(validation.size(), 1) << validation.dump(4);
    EXPECT_EQ(validation["param"]["value"], 1);
    EXPECT_EQ(validation["param"]["requiresRestart"], true);
}

TEST(ConfigurationTest, CompatVersionComparison) {
    using cb::config::FeatureVersion;
    EXPECT_EQ(FeatureVersion(8, 0), FeatureVersion(8, 0));
    EXPECT_LT(FeatureVersion(8, 0), FeatureVersion(8, 1));
    EXPECT_GE(FeatureVersion(8, 0), FeatureVersion(7, 6));
}

TEST(ConfigurationTest, CompatVersionDefault) {
    using cb::config::FeatureVersion;
    ConfigurationShim configuration;
    auto version = configuration.getEffectiveCompatVersion();
    EXPECT_EQ(version, FeatureVersion::max());
}

TEST(ConfigurationTest, CompatVersionSet) {
    using cb::config::FeatureVersion;
    ConfigurationShim configuration;
    configuration.setParameter("compat_version", "8.0");
    auto version = configuration.getEffectiveCompatVersion();
    EXPECT_EQ(version, FeatureVersion(8, 0));

    configuration.setParameter("compat_version", "");
    auto versionMax = configuration.getEffectiveCompatVersion();
    EXPECT_EQ(versionMax, FeatureVersion::max());
}

TEST(ConfigurationTest, CompatVersionSetInvalid) {
    using cb::config::FeatureVersion;
    ConfigurationShim configuration;
    EXPECT_THROW(configuration.setParameter("compat_version", "asd"),
                 std::invalid_argument);
    EXPECT_THROW(configuration.setParameter("compat_version", "8"),
                 std::invalid_argument);
    EXPECT_THROW(configuration.setParameter("compat_version", "8.0.0"),
                 std::invalid_argument);
    EXPECT_EQ(configuration.getEffectiveCompatVersion(), FeatureVersion::max());
}
