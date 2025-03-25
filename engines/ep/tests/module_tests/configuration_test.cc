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
#include "memcached/config_parser.h"
#include "memcached/server_core_iface.h"
#include "programs/engine_testapp/mock_server.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <memory>

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
    SizeRangeValidator validator;
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
    using Configuration::addParameter;
    using Configuration::getParameter;
    using Configuration::setParameter;
};

TEST(ConfigurationTest, SetGetWorks) {
    ConfigurationShim configuration;

    configuration.addParameter("bool", false, false);
    EXPECT_EQ(configuration.getParameter<bool>("bool"), false);

    configuration.addParameter("size", (size_t)100, false);
    EXPECT_EQ(configuration.getParameter<size_t>("size"), 100);

    configuration.addParameter("ssize", (ssize_t)-100, false);
    EXPECT_EQ(configuration.getParameter<ssize_t>("ssize"), -100);

    configuration.addParameter("float", (float)123.5, false);
    EXPECT_EQ(configuration.getParameter<float>("float"), 123.5);

    configuration.addParameter("string", std::string("hello"), false);
    EXPECT_EQ(configuration.getParameter<std::string>("string"), "hello");
}

TEST(ConfigurationTest, ValidatorWorks) {
    ConfigurationShim configuration;
    std::string key{"test_key"};

    configuration.addParameter(key, (size_t)110, false);
    EXPECT_NO_THROW(configuration.setValueValidator(key, (new SizeRangeValidator())->min(10)->max(100)));
    EXPECT_NO_THROW(configuration.setParameter(key, (size_t)10));
    EXPECT_NO_THROW(configuration.setParameter(key, (size_t)100));
    EXPECT_THROW(configuration.setParameter(key, (size_t)9), std::range_error);

    CB_EXPECT_THROW_MSG(configuration.setParameter(key, (size_t)9),
                        std::range_error,
                        "Validation Error, test_key takes values between 10 "
                        "and 100 (Got: 9)");
}
class MockValueChangedListener : public ValueChangedListener {
public:
    MOCK_METHOD2(booleanValueChanged, void(const std::string&, bool));
    MOCK_METHOD2(sizeValueChanged, void(const std::string&, size_t));
    MOCK_METHOD2(ssizeValueChanged, void(const std::string&, ssize_t));
    MOCK_METHOD2(floatValueChanged, void(const std::string&, float));
    MOCK_METHOD2(stringValueChanged, void(const std::string&, const char*));
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
    configuration.addParameter(key, (ssize_t)1, false);

    EXPECT_CALL(*mvcl, ssizeValueChanged("test_key", 2)).Times(1);
    EXPECT_CALL(*mvcl, sizeValueChanged(_, _)).Times(0);

    // add listeners
    configuration.addValueChangedListener(key, std::move(mvcl));

    // change parameters
    configuration.setParameter(key, (ssize_t)2);
}

class UnknownKeyCallbackMock {
public:
    MOCK_METHOD2(Callback, void(std::string_view key, std::string_view value));
};

class ConfigurationParseTest : public ::testing::Test {
protected:
    void SetUp() override {
        configuration = std::make_unique<ConfigurationShim>();
        mock_server_api = get_mock_server_api();
        unknownKeyCallbackMock = std::make_unique<UnknownKeyCallbackMock>();

        // Two attributes
        items.resize(2);
        items[0].key = "alog_sleep_time";
        items[0].datatype = DT_SIZE;
        items[0].value.dt_size = new size_t(200);
        items[1].key = "different_key";
        items[1].datatype = DT_SIZE;
    }

    void TearDown() override {
        delete items[0].value.dt_size;
    }

    std::unique_ptr<ConfigurationShim> configuration;
    ServerApi* mock_server_api;
    std::unique_ptr<UnknownKeyCallbackMock> unknownKeyCallbackMock;
    std::vector<config_item> items;
};

TEST_F(ConfigurationParseTest, KnownKey) {
    auto callback = [this](std::string_view key, std::string_view value) {
        unknownKeyCallbackMock->Callback(key, value);
    };

    // Unknown key callback should not be called
    EXPECT_CALL(*unknownKeyCallbackMock, Callback("alog_sleep_time", "100"))
            .Times(0);

    // parse_config returns 0 if the key is found
    EXPECT_EQ(0,
              mock_server_api->core->parse_config(
                      "alog_sleep_time=100;", items.data(), nullptr, callback));

    // parseConfiguration should return true
    EXPECT_TRUE(configuration->parseConfiguration("alog_sleep_time=100;",
                                                  get_mock_server_api()));
}

TEST_F(ConfigurationParseTest, UnknownKey) {
    auto callback = [this](std::string_view key, std::string_view value) {
        // Correct key and value
        EXPECT_EQ("unknown_key", key);
        EXPECT_EQ("100", value);
        unknownKeyCallbackMock->Callback(key, value);
    };

    // Should be called once
    EXPECT_CALL(*unknownKeyCallbackMock, Callback("unknown_key", "100"))
            .Times(1);

    // parse_config returns 1 if the key is not found/unsupported
    EXPECT_EQ(1,
              mock_server_api->core->parse_config(
                      "unknown_key=100;", items.data(), nullptr, callback));

    // parseConfiguration should still return true if key is not found
    EXPECT_TRUE(configuration->parseConfiguration("unknown_key=100;",
                                                  get_mock_server_api()));
}

TEST_F(ConfigurationParseTest, ErrorKey) {
    auto callback = [this](std::string_view key, std::string_view value) {
        unknownKeyCallbackMock->Callback(key, value);
    };

    // Unknown key callback should not be called
    EXPECT_CALL(*unknownKeyCallbackMock, Callback("alog_sleep_time", "100"))
            .Times(0);

    // parse_config returns -1 on error
    EXPECT_EQ(-1,
              mock_server_api->core->parse_config(
                      "alog_sleep_time=false;", // wrong type
                      items.data(),
                      nullptr,
                      callback));

    // parseConfiguration should return false on error
    EXPECT_FALSE(configuration->parseConfiguration("alog_sleep_time=false;",
                                                   get_mock_server_api()));
}