/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "clustertest.h"

#include "bucket.h"
#include "cluster.h"
#include <include/mcbp/protocol/unsigned_leb128.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <thread>

class CollectionsTests : public cb::test::ClusterTest {
protected:
    static std::unique_ptr<MemcachedConnection> getConnection() {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        conn->setFeature(cb::mcbp::Feature::Collections, true);
        return conn;
    }

    std::string createKey(CollectionIDType cid, std::string key) {
        cb::mcbp::unsigned_leb128<CollectionIDType> leb(cid);
        std::string ret;
        std::copy(leb.begin(), leb.end(), std::back_inserter(ret));
        ret.append(key);
        return ret;
    }

    static void mutate(MemcachedConnection& conn,
                       std::string id,
                       MutationType type) {
        Document doc{};
        doc.value = "body";
        doc.info.id = std::move(id);
        doc.info.datatype = cb::mcbp::Datatype::Raw;
        const auto info = conn.mutate(doc, Vbid{0}, type);
        EXPECT_NE(0, info.cas);
    }
};

// Verify that I can store documents within the collections
TEST_F(CollectionsTests, TestBasicOperations) {
    auto conn = getConnection();
    mutate(*conn, createKey(8, "TestBasicOperations"), MutationType::Add);
    mutate(*conn, createKey(8, "TestBasicOperations"), MutationType::Set);
    mutate(*conn, createKey(8, "TestBasicOperations"), MutationType::Replace);
}

TEST_F(CollectionsTests, TestUnknownScope) {
    auto conn = getConnection();
    try {
        mutate(*conn, createKey(1, "TestBasicOperations"), MutationType::Add);
        FAIL() << "Invalid scope not detected";
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::UnknownCollection, e.getReason());
    }
}
