/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <cstdlib>
#include <iostream>
#include <map>

#include <cerrno>
#include <cstring>
#include "auditconfig.h"
#include "mock_auditconfig.h"

#include <folly/portability/GTest.h>

class AuditConfigTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        testdir = cb::io::mkdtemp("auditconfig-test-");
    }

    static void TearDownTestCase() {
        try {
            cb::io::rmrf(testdir);
        } catch (const std::exception&) {
        }
    }

    nlohmann::json json;
    AuditConfig config;
    static std::string testdir;

    void SetUp() override {
        json = createDefaultConfig();
    }

    void TearDown() override {
    }

    nlohmann::json createDefaultConfig() {
        nlohmann::json root;
        root["version"] = 2;
        root["rotate_size"] = 20 * 1024 * 1024;
        root["rotate_interval"] = 900;
        root["auditd_enabled"] = true;
        root["buffered"] = true;
        root["log_path"] = testdir;
        nlohmann::json sync = nlohmann::json::array();
        root["sync"] = sync;
        nlohmann::json disabled = nlohmann::json::array();
        root["disabled"] = disabled;
        nlohmann::json event_states;
        root["event_states"] = event_states;
        nlohmann::json disabled_userids = nlohmann::json::array();
        root["disabled_userids"] = disabled_userids;
        root["filtering_enabled"] = true;
        root["uuid"] = "123456";

        return root;
    }
};

std::string AuditConfigTest::testdir;

TEST_F(AuditConfigTest, UnknownTag) {
    json["foo"] = 5;
    EXPECT_THROW(AuditConfig cfg(json), std::invalid_argument);
}

// version

TEST_F(AuditConfigTest, TestGetVersion) {
    config = AuditConfig(json);
    EXPECT_EQ(2, config.get_version());
}

TEST_F(AuditConfigTest, TestNoVersion) {
    json.erase("version");
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestIllegalDatatypeVersion) {
    json["version"] = "foobar";
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestLegalVersion) {
    for (int version = -100; version < 100; ++version) {
        json["version"] = version;
        if (version == 2) {
            json["filtering_enabled"] = true;
            nlohmann::json disabled_userids = nlohmann::json::array();
            json["disabled_userids"] = disabled_userids;
            nlohmann::json event_states;
            json["event_states"] = event_states;
            json["uuid"] = "123456";
        }
        if (version == 2) {
            config = AuditConfig(json);
        } else {
            EXPECT_THROW(AuditConfig cfg(json), std::invalid_argument);
        }
    }
}

// rotate_size

TEST_F(AuditConfigTest, TestNoRotateSize) {
    json.erase("rotate_size");
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestRotateSizeSetGet) {
    for (size_t ii = 0; ii < 100; ++ii) {
        config.set_rotate_size(ii);
        EXPECT_EQ(ii, config.get_rotate_size());
    }
}

TEST_F(AuditConfigTest, TestRotateSizeIllegalDatatype) {
    json["rotate_size"] = "foobar";
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestRotateSizeLegalValue) {
    json["rotate_size"] = 100;
    config = AuditConfig(json);
}

TEST_F(AuditConfigTest, TestRotateSizeIllegalValue) {
    json["rotate_size"] = -1;
    EXPECT_THROW(AuditConfig cfg(json), std::invalid_argument);
}

// rotate_interval

TEST_F(AuditConfigTest, TestNoRotateInterval) {
    json.erase("rotate_interval");
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestDisableRotateInterval) {
    json["rotate_interval"] = 0;
    config = AuditConfig(json);
    EXPECT_EQ(0, config.get_rotate_interval());
}

TEST_F(AuditConfigTest, TestRotateIntervalSetGet) {
    const uint32_t min_file_rotation_time = AuditConfig::min_file_rotation_time;
    for (size_t ii = min_file_rotation_time;
         ii < min_file_rotation_time + 10;
         ++ii) {
        config.set_rotate_interval(uint32_t(ii));
        EXPECT_EQ(ii, config.get_rotate_interval());
    }
}

TEST_F(AuditConfigTest, TestRotateIntervalIllegalDatatype) {
    json["rotate_interval"] = "foobar";
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestRotateIntervalLegalValue) {
    for (uint32_t ii = AuditConfig::min_file_rotation_time;
         ii < AuditConfig::max_file_rotation_time;
         ii += 1000) {
        json["rotate_interval"] = ii;
        config = AuditConfig(json);
    }
}

TEST_F(AuditConfigTest, TestRotateIntervalIllegalValue) {
    json["rotate_interval"] = AuditConfig::min_file_rotation_time - 1;
    EXPECT_THROW(AuditConfig cfg(json), std::invalid_argument);
    json["rotate_interval"] = AuditConfig::max_file_rotation_time + 1;
    EXPECT_THROW(AuditConfig cfg(json), std::invalid_argument);
}

// auditd_enabled

TEST_F(AuditConfigTest, TestNoAuditdEnabled) {
    json.erase("auditd_enabled");
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestGetSetAuditdEnabled) {
    config.set_auditd_enabled(true);
    EXPECT_TRUE(config.is_auditd_enabled());
    config.set_auditd_enabled(false);
    EXPECT_FALSE(config.is_auditd_enabled());
}

TEST_F(AuditConfigTest, TestIllegalDatatypeAuditdEnabled) {
    json["auditd_enabled"] = "foobar";
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestLegalAuditdEnabled) {
    json["auditd_enabled"] = true;
    config = AuditConfig(json);

    json["auditd_enabled"] = false;
    config = AuditConfig(json);
}

// buffered

TEST_F(AuditConfigTest, TestNoBuffered) {
    // buffered is optional, and enabled unless explicitly disabled
    json.erase("buffered");
    config = AuditConfig(json);
    EXPECT_TRUE(config.is_buffered());
}

TEST_F(AuditConfigTest, TestGetSetBuffered) {
    config.set_buffered(true);
    EXPECT_TRUE(config.is_buffered());
    config.set_buffered(false);
    EXPECT_FALSE(config.is_buffered());
}

TEST_F(AuditConfigTest, TestIllegalDatatypeBuffered) {
    json["buffered"] = "foobar";
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestLegalBuffered) {
    json["buffered"] = true;
    config = AuditConfig(json);

    json["buffered"] = false;
    config = AuditConfig(json);
}

// log_path

TEST_F(AuditConfigTest, TestNoLogPath) {
    json.erase("log_path");
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestGetSetLogPath) {
    config.set_log_directory(testdir);
    EXPECT_EQ(testdir, config.get_log_directory());
}

TEST_F(AuditConfigTest, TestGetSetSanitizeLogPath) {
    // Trim of trailing paths
    std::string path = testdir + std::string("/");
    config.set_log_directory(path);
    EXPECT_EQ(testdir, config.get_log_directory());
}

#ifdef WIN32
TEST_F(AuditConfigTest, TestGetSetSanitizeLogPathMixedSeparators) {
    // Trim of trailing paths
    std::string path = testdir + std::string("/mydir\\baddir");
    config.set_log_directory(path);
    EXPECT_EQ(testdir + "\\mydir\\baddir", config.get_log_directory());
}
#endif

// Sync

TEST_F(AuditConfigTest, TestNoSync) {
    json.erase("sync");
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestSpecifySync) {
    nlohmann::json array = nlohmann::json::array();
    for (int ii = 0; ii < 10; ++ii) {
        array.push_back(ii);
    }
    json["sync"] = array;
    config = AuditConfig(json);

    for (uint32_t ii = 0; ii < 100; ++ii) {
        if (ii < 10) {
            EXPECT_TRUE(config.is_event_sync(ii));
        } else {
            EXPECT_FALSE(config.is_event_sync(ii));
        }
    }
}

// Disabled

TEST_F(AuditConfigTest, TestNoDisabled) {
    json.erase("disabled");
    if (config.get_version() == 1) {
        EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
    } else {
        config = AuditConfig(json);
    }
}

TEST_F(AuditConfigTest, TestSpecifyDisabledUsers) {
    nlohmann::json array = nlohmann::json::array();
    for (uint16_t ii = 0; ii < 10; ++ii) {
        nlohmann::json userIdRoot;

        // In version 2 of the configuration we support domain or support
        // however domain is the preferred notation.
        // Have 10 users so make half use domain and half use source
        if (ii < 5) {
            userIdRoot["domain"] = "internal";
        } else {
            userIdRoot["source"] = "internal";
        }
        auto user = "user" + std::to_string(ii);
        userIdRoot["user"] = user;
        array.push_back(userIdRoot);
    }
    json["disabled_userids"] = array;
    config = AuditConfig(json);

    for (uint16_t ii = 0; ii < 100; ++ii) {
        const auto& domain = "internal";
        const auto& user = "user" + std::to_string(ii);
        const auto& userid = std::make_pair(domain, user);
        if (ii < 10) {
            EXPECT_TRUE(config.is_event_filtered(userid));
        } else {
            EXPECT_FALSE(config.is_event_filtered(userid));
        }
    }
}

/**
 * Tests that when converting a config containing a single disabled_user it
 * translates to a single entry in the json "disabled_userids" array and the
 * json "disabled" array remains empty.
 */
TEST_F(AuditConfigTest, AuditConfigDisabledUsers) {
    MockAuditConfig config;

    nlohmann::json disabledUserids = nlohmann::json::array();
    nlohmann::json userIdRoot;
    userIdRoot["domain"] = "internal";
    userIdRoot["user"] = "johndoe";
    disabledUserids.push_back(userIdRoot);
    config.public_set_disabled_userids(disabledUserids);

    auto json = config.to_json();
    auto disabledUseridsArray = json["disabled_userids"];
    EXPECT_EQ(1, disabledUseridsArray.size());
    auto disabledArray = json["disabled"];
    EXPECT_EQ(0, disabledArray.size());
}

// Test the filtering_enabled parameter

TEST_F(AuditConfigTest, TestNoFilteringEnabled) {
    json.erase("filtering_enabled");
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestGetSetFilteringEnabled) {
    config.set_filtering_enabled(true);
    EXPECT_TRUE(config.is_filtering_enabled());
    config.set_filtering_enabled(false);
    EXPECT_FALSE(config.is_filtering_enabled());
}

TEST_F(AuditConfigTest, TestIllegalDatatypeFilteringEnabled) {
    json["filtering_enabled"] = "foobar";
    EXPECT_THROW(AuditConfig cfg(json), nlohmann::json::exception);
}

TEST_F(AuditConfigTest, TestLegalFilteringEnabled) {
    json["filtering_enabled"] = true;
    config = AuditConfig(json);

    json["filtering_enabled"] = false;
    config = AuditConfig(json);
}

// The event_states list is optional and therefore if it does not exist
// it should not throw an exception.
TEST_F(AuditConfigTest, TestNoEventStates) {
    json.erase("event_states");
    config = AuditConfig(json);
}

// Test that with an event_states object consisting of "enabled" and "disabled"
// the states get converted into corresponding EventStates.  Also if an event
// is not in the event_states object it has an EventState of undefined.
TEST_F(AuditConfigTest, TestSpecifyEventStates) {
    nlohmann::json object;
    for (int ii = 0; ii < 5; ++ii) {
        auto event = std::to_string(ii);
        object[event] = "enabled";
    }
    for (int ii = 5; ii < 10; ++ii) {
        auto event = std::to_string(ii);
        object[event] = "disabled";
    }
    json["event_states"] = object;
    config = AuditConfig(json);

    for (uint32_t ii = 0; ii < 20; ++ii) {
        if (ii < 5) {
            EXPECT_EQ(AuditConfig::EventState::enabled,
                      config.get_event_state(ii));
        } else if (ii < 10) {
            EXPECT_EQ(AuditConfig::EventState::disabled,
                      config.get_event_state(ii));
        } else {
            EXPECT_EQ(AuditConfig::EventState::undefined,
                      config.get_event_state(ii));
        }
    }
}
