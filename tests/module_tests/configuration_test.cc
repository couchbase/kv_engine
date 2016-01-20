/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "configuration.h"

#include <gtest/gtest.h>

void Configuration::initialize() {}

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

    (&validator)->min(-100.1)->max(1000.1);

    /* In-bounds */
    EXPECT_NO_THROW(validator.validateFloat(key, -100.1));
    EXPECT_NO_THROW(validator.validateFloat(key, 100.00));
    EXPECT_NO_THROW(validator.validateFloat(key, 101.0));
    EXPECT_NO_THROW(validator.validateFloat(key, 999.0));

    /* Out-of bounds */
    CB_EXPECT_THROW_MSG(
            validator.validateFloat(key, -100.2),
            std::range_error,
            ("Validation Error, test_key takes values between "
             + std::to_string(-100.1f)
             + " and " + std::to_string(1000.1f)
             + " (Got: " + std::to_string(-100.2f) + ")").c_str()
    );
    CB_EXPECT_THROW_MSG(
            validator.validateFloat(key, 1000.2),
            std::range_error,
            ("Validation Error, test_key takes values between "
             + std::to_string(-100.1f)
             + " and " + std::to_string(1000.1f)
             + " (Got: " + std::to_string(1000.2f) + ")").c_str()
    );
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

    void usetParameter(const std::string &key, bool value) {
        setParameter(key, value);
    }
    void usetParameter(const std::string &key, size_t value) {
        setParameter(key, value);
    }
    void usetParameter(const std::string &key, ssize_t value) {
        setParameter(key, value);
    }
    void usetParameter(const std::string &key, float value) {
        setParameter(key, value);
    }
    void usetParameter(const std::string &key, const char *value) {
        setParameter(key, value);
    }
    void usetParameter(const std::string &key, const std::string &value) {
        setParameter(key, value);
    }

    bool ugetBool(const std::string &key) const {
        return getBool(key);
    }
    size_t ugetInteger(const std::string &key) const {
        return getInteger(key);
    }

    ssize_t ugetSignedInteger(const std::string &key) const {
        return getSignedInteger(key);
    }
    float ugetFloat(const std::string &key) const {
        return getFloat(key);
    }
    std::string ugetString(const std::string &key) const {
        return getString(key);
    }
};

TEST(ConfigurationTest, SetGetWorks) {
    ConfigurationShim configuration;

    configuration.usetParameter("bool", false);
    EXPECT_EQ(configuration.ugetBool("bool"), false);

    configuration.usetParameter("size", (size_t) 100);
    EXPECT_EQ(configuration.ugetInteger("size"), 100);

    configuration.usetParameter("ssize", (ssize_t) -100);
    EXPECT_EQ(configuration.ugetSignedInteger("ssize"), -100);

    configuration.usetParameter("float", (float) 123.5);
    EXPECT_EQ(configuration.ugetFloat("float"), 123.5);

    configuration.usetParameter("char*", "hello");
    EXPECT_EQ(configuration.ugetString("char*"), "hello");

    configuration.usetParameter("string", std::string("hello"));
    EXPECT_EQ(configuration.ugetString("string"), "hello");
}

TEST(ConfigurationTest, ValidatorWorks) {
    ConfigurationShim configuration;
    std::string key{"test_key"};

    configuration.usetParameter(key, (size_t) 110);
    EXPECT_NO_THROW(configuration.setValueValidator(key, (new SizeRangeValidator())->min(10)->max(100)));
    EXPECT_NO_THROW(configuration.usetParameter(key, (size_t) 10));
    EXPECT_NO_THROW(configuration.usetParameter(key, (size_t) 100));
    EXPECT_THROW(configuration.usetParameter(key, (size_t) 9), std::range_error);

    CB_EXPECT_THROW_MSG(
            configuration.usetParameter(key, (size_t) 9),
            std::range_error,
            "Validation Error, test_key takes values between 10 and 100 (Got: 9)"
    );

}
