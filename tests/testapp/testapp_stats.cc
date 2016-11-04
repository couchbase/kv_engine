/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "testapp_stats.h"

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        StatsTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpIpv6Plain,
                                          TransportProtocols::McbpSsl,
                                          TransportProtocols::McbpIpv6Ssl
                                         ),
                        ::testing::PrintToStringParamName());

TEST_P(StatsTest, TestDefaultStats) {
    MemcachedConnection& conn = getConnection();
    unique_cJSON_ptr stats;
    stats = conn.stats("");

    // Don't expect the entire stats set, but we should at least have
    // the pid
    EXPECT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "pid"));
}

TEST_P(StatsTest, DISABLED_StatsResetIsPrivileged) {
    MemcachedConnection& conn = getConnection();
    unique_cJSON_ptr stats;

    try {
        conn.stats("reset");
        FAIL() << "reset is a privileged operation";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }

    conn.authenticate("_admin", "password", "PLAIN");
    conn.stats("reset");
    conn.reconnect();
    ASSERT_THROW(conn.stats("reset"), ConnectionError);
}

TEST_P(StatsTest, TestReset) {
    MemcachedConnection& conn = getConnection();
    unique_cJSON_ptr stats;

    stats = conn.stats("");
    ASSERT_NE(nullptr, stats.get());

    auto* count = cJSON_GetObjectItem(stats.get(), "cmd_get");
    ASSERT_NE(nullptr, count);
    EXPECT_EQ(cJSON_Number, count->type);
    auto before = count->valueint;

    for (int ii = 0; ii < 10; ++ii) {
        EXPECT_THROW(conn.get("foo", 0), ConnectionError);
    }

    stats = conn.stats("");
    count = cJSON_GetObjectItem(stats.get(), "cmd_get");
    ASSERT_NE(nullptr, count);
    EXPECT_EQ(cJSON_Number, count->type);
    EXPECT_NE(before, count->valueint);

    // the cmd_get counter does work.. now check that reset sets it back..
    resetBucket();

    stats = conn.stats("");
    count = cJSON_GetObjectItem(stats.get(), "cmd_get");
    ASSERT_NE(nullptr, count);
    EXPECT_EQ(cJSON_Number, count->type);
    EXPECT_EQ(0, count->valueint);

    // Just ensure that the "reset timings" is detected
    // @todo add a separate test case for cmd timings stats
    conn.authenticate("_admin", "password", "PLAIN");
    conn.selectBucket("default");
    stats = conn.stats("reset timings");

    // Just ensure that the "reset bogus" is detected..
    try {
        conn.stats("reset bogus");
        FAIL()<<"stats reset bogus should throw an exception (non a valid cmd)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments());
    }
    conn.reconnect();
}

/**
 * MB-17815: The cmd_set stat is incremented multiple times if the underlying
 * engine returns EWOULDBLOCK (which would happen for all operations when
 * the underlying engine is operating in full eviction mode and the document
 * isn't resident)
 */
TEST_P(StatsTest, Test_MB_17815) {
    MemcachedConnection& conn = getConnection();

    unique_cJSON_ptr stats;

    stats = conn.stats("");
    auto* count = cJSON_GetObjectItem(stats.get(), "cmd_set");
    ASSERT_NE(nullptr, count);
    EXPECT_EQ(cJSON_Number, count->type);
    EXPECT_EQ(0, count->valueint);

    conn.configureEwouldBlockEngine(EWBEngineMode::Sequence,
                                    ENGINE_EWOULDBLOCK,
                                    0xfffffffd);

    Document doc;
    doc.info.cas = Greenstack::CAS::Wildcard;
    doc.info.compression = Greenstack::Compression::None;
    doc.info.datatype = Greenstack::Datatype::Json;
    doc.info.flags = 0xcaffee;
    doc.info.id = name;
    char* ptr = cJSON_Print(memcached_cfg.get());
    std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
    cJSON_Free(ptr);

    conn.mutate(doc, 0, Greenstack::MutationType::Add);
    stats = conn.stats("");
    count = cJSON_GetObjectItem(stats.get(), "cmd_set");
    ASSERT_NE(nullptr, count);
    EXPECT_EQ(cJSON_Number, count->type);
    EXPECT_EQ(1, count->valueint);
}


TEST_P(StatsTest, TestSettings) {
    MemcachedConnection& conn = getConnection();
    // @todo verify that I get all of the expected settings. for now
    //       just verify that I've got the ones I expect...
    unique_cJSON_ptr stats;
    ASSERT_NO_THROW(stats = conn.stats("settings"));
    ASSERT_NE(nullptr, stats.get());
    ASSERT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "maxconns"));

    // skip interfaces....

    ASSERT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "verbosity"));
    ASSERT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "num_threads"));
    ASSERT_NE(nullptr,
              cJSON_GetObjectItem(stats.get(), "reqs_per_event_high_priority"));
    ASSERT_NE(nullptr,
              cJSON_GetObjectItem(stats.get(), "reqs_per_event_med_priority"));
    ASSERT_NE(nullptr,
              cJSON_GetObjectItem(stats.get(), "reqs_per_event_low_priority"));
    ASSERT_NE(nullptr,
              cJSON_GetObjectItem(stats.get(), "reqs_per_event_def_priority"));
    ASSERT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "auth_enabled_sasl"));
    ASSERT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "auth_sasl_engine"));
    ASSERT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "auth_required_sasl"));

    // skip extensions, loggers and daemons

    // Skip audit.. it is "optional" and we don't pass it to the config
}

TEST_P(StatsTest, DISABLED_TestAuditNoAccess) {
    MemcachedConnection& conn = getConnection();

    try {
        conn.stats("audit");
        FAIL() << "stats audit should throw an exception (non privileged)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(StatsTest, TestAudit) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("_admin", "password", "PLAIN");

    unique_cJSON_ptr stats;
    stats = conn.stats("audit");
    EXPECT_NE(nullptr, stats.get());
    EXPECT_EQ(2, cJSON_GetArraySize(stats.get()));

    auto* enabled = cJSON_GetObjectItem(stats.get(), "enabled");
    EXPECT_NE(nullptr, enabled) << "Missing field \"enabled\"";
    EXPECT_EQ(cJSON_False, enabled->type);

    auto* dropped = cJSON_GetObjectItem(stats.get(), "dropped_events");
    EXPECT_NE(nullptr, dropped) << "Missing field \"dropped_events\"";
    EXPECT_EQ(cJSON_Number, dropped->type);
    EXPECT_EQ(0, dropped->valueint);

    conn.reconnect();
}

TEST_P(StatsTest, DISABLED_TestBucketDetailsNoAccess) {
    MemcachedConnection& conn = getConnection();

    try {
        conn.stats("bucket_details");
        FAIL() <<
               "stats bucket_details should throw an exception (non privileged)";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied());
    }
}

TEST_P(StatsTest, TestBucketDetails) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate("_admin", "password", "PLAIN");

    unique_cJSON_ptr stats;
    stats = conn.stats("bucket_details");
    ASSERT_NE(nullptr, stats.get());
    ASSERT_EQ(1, cJSON_GetArraySize(stats.get()));
    std::string key(stats.get()->child->string);
    ASSERT_EQ("bucket details", key);
    unique_cJSON_ptr json(cJSON_Parse(stats.get()->child->valuestring));

    // bucket details contains a single entry which is named "buckets" and
    // contains an arrray
    cJSON* array = cJSON_GetObjectItem(json.get(), "buckets");
    EXPECT_EQ(cJSON_Array, array->type);

    // we have two bucket2, nobucket and default
    EXPECT_EQ(2, cJSON_GetArraySize(array));

    // Validate each bucket entry (I should probably extend it with checking
    // of the actual values
    for (cJSON* bucket = array->child;
         bucket != nullptr; bucket = bucket->next) {
        EXPECT_EQ(5, cJSON_GetArraySize(bucket));
        EXPECT_NE(nullptr, cJSON_GetObjectItem(bucket, "index"));
        EXPECT_NE(nullptr, cJSON_GetObjectItem(bucket, "state"));
        EXPECT_NE(nullptr, cJSON_GetObjectItem(bucket, "clients"));
        EXPECT_NE(nullptr, cJSON_GetObjectItem(bucket, "name"));
        EXPECT_NE(nullptr, cJSON_GetObjectItem(bucket, "type"));
    }

    conn.reconnect();
}

TEST_P(StatsTest, TestAggregate) {
    MemcachedConnection& conn = getConnection();
    try {
        auto stats = conn.stats("aggregate");
        // Don't expect the entire stats set, but we should at least have
        // the pid
        EXPECT_NE(nullptr, cJSON_GetObjectItem(stats.get(), "pid"));
    } catch (...) {
        FAIL() << "Failed to fetch the aggregate stats";
    }
}

TEST_P(StatsTest, DISABLED_TestConnections) {
    MemcachedConnection& conn = getConnection();
    unique_cJSON_ptr stats;
    stats = conn.stats("connections");
    ASSERT_NE(nullptr, stats.get());
    // We have at _least_ 2 connections
    ASSERT_LE(2, cJSON_GetArraySize(stats.get()));

    int sock = -1;

    // Unfortuately they're all mapped as a " " : "json" pairs, so lets
    // validate that at least thats true:
    for (auto* conn = stats.get()->child; conn != nullptr; conn = conn->next) {
        unique_cJSON_ptr json(cJSON_Parse(conn->valuestring));
        ASSERT_NE(nullptr, json.get());
        // the _this_ pointer should at least be there
        ASSERT_NE(nullptr, cJSON_GetObjectItem(json.get(), "connection"));
        if (sock == -1) {
            auto* ptr = cJSON_GetObjectItem(json.get(), "socket");
            if (ptr != nullptr) {
                EXPECT_EQ(cJSON_Number, ptr->type);
                sock = ptr->valueint;
            }
        }
    }

    ASSERT_NE(-1, sock) << "Failed to locate a single connection object";

    EXPECT_NO_THROW(stats = conn.stats("connections " + std::to_string(sock)));
    ASSERT_NE(nullptr, stats.get());
    ASSERT_EQ(1, cJSON_GetArraySize(stats.get()));
    unique_cJSON_ptr json(cJSON_Parse(stats.get()->child->valuestring));
    ASSERT_NE(nullptr, json.get());
    auto* ptr = cJSON_GetObjectItem(json.get(), "socket");
    ASSERT_NE(nullptr, ptr);
    EXPECT_EQ(cJSON_Number, ptr->type);
    EXPECT_EQ(sock, ptr->valueint);
}

TEST_P(StatsTest, TestConnectionsInvalidNumber) {
    MemcachedConnection& conn = getConnection();
    try {
        auto stats = conn.stats("connections xxx");
        FAIL() << "Did not detect incorrect connection number";

    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isInvalidArguments());
    }
}

TEST_P(StatsTest, TestTopkeys) {
    MemcachedConnection& conn = getConnection();

    try {
        for (int ii = 0; ii < 10; ++ii) {
            Document doc;
            doc.info.cas = Greenstack::CAS::Wildcard;
            doc.info.compression = Greenstack::Compression::None;
            doc.info.datatype = Greenstack::Datatype::Json;
            doc.info.flags = 0xcaffee;
            doc.info.id = name;
            char* ptr = cJSON_Print(memcached_cfg.get());
            std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
            cJSON_Free(ptr);

            EXPECT_NO_THROW(conn.mutate(doc, 0, Greenstack::MutationType::Set));
        }

        auto stats = conn.stats("topkeys");
        cJSON* elem = cJSON_GetObjectItem(stats.get(), name.c_str());
        EXPECT_NE(nullptr, elem);
    } catch (...) {
        FAIL() << "Failed to fetch the topkeys stats";
    }
}

TEST_P(StatsTest, TestTopkeysJson) {
    MemcachedConnection& conn = getConnection();

    try {
        for (int ii = 0; ii < 10; ++ii) {
            Document doc;
            doc.info.cas = Greenstack::CAS::Wildcard;
            doc.info.compression = Greenstack::Compression::None;
            doc.info.datatype = Greenstack::Datatype::Json;
            doc.info.flags = 0xcaffee;
            doc.info.id = name;
            char* ptr = cJSON_Print(memcached_cfg.get());
            std::copy(ptr, ptr + strlen(ptr), std::back_inserter(doc.value));
            cJSON_Free(ptr);

            EXPECT_NO_THROW(conn.mutate(doc, 0, Greenstack::MutationType::Set));
        }

        auto stats = conn.stats("topkeys_json");
        auto* topkeys = cJSON_GetObjectItem(stats.get(), "topkeys_json");
        ASSERT_NE(nullptr, topkeys);
        unique_cJSON_ptr value(cJSON_Parse(topkeys->valuestring));
        ASSERT_NE(nullptr, value.get());
        bool found = false;
        cJSON* items = cJSON_GetObjectItem(value.get(), "topkeys");
        for (auto* child = items->child;
             child != nullptr; child = child->next) {
            auto* key = cJSON_GetObjectItem(child, "key");
            ASSERT_NE(nullptr, key);
            std::string val(key->valuestring);
            if (name == val) {
                found = true;
                break;
            }
        }

        EXPECT_TRUE(found);
    } catch (...) {
        FAIL() << "Failed to fetch the default number of stats";
    }
}

TEST_P(StatsTest, TestSubdocExecute) {
    MemcachedConnection& conn = getConnection();
    unique_cJSON_ptr stats;
    stats = conn.stats("subdoc_execute");

    // @todo inspect the content. for now just validate that we've got a
    //       single element in there..
    EXPECT_EQ(1, cJSON_GetArraySize(stats.get()));
    std::string value(stats.get()->child->valuestring);
    EXPECT_EQ(0, value.find("{\"ns\":"));
}
