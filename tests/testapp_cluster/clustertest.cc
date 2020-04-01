/*
 *     Copyright 2019 Couchbase, Inc
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

#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

std::unique_ptr<cb::test::Cluster> cb::test::ClusterTest::cluster;

std::shared_ptr<cb::test::Bucket> cb::test::UpgradeTest::bucket;

void cb::test::ClusterTest::SetUpTestCase() {
    cluster = Cluster::create(4);
    if (!cluster) {
        std::cerr << "Failed to create the cluster" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    try {
        createDefaultBucket();
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}

void cb::test::ClusterTest::TearDownTestCase() {
    cluster.reset();
}

void cb::test::ClusterTest::createDefaultBucket() {
    auto bucket = cluster->createBucket("default",
                                        {{"replicas", 2}, {"max_vbuckets", 8}});
    if (!bucket) {
        throw std::runtime_error("Failed to create default bucket");
    }
    bucket->setCollectionManifest(R"({
  "uid": "1",
  "scopes": [
    {
      "name": "_default",
      "uid": "0",
      "collections": [
        {
          "name": "_default",
          "uid": "0"
        },
        {
          "name": "fruit",
          "uid": "8"
        },
        {
          "name": "vegetable",
          "uid": "9"
        }
      ]
    }
  ]
})"_json);
}

void cb::test::ClusterTest::SetUp() {
    Test::SetUp();
}

void cb::test::ClusterTest::TearDown() {
    Test::TearDown();
}

void cb::test::ClusterTest::getReplica(MemcachedConnection& conn,
                                       Vbid vbid,
                                       const std::string& key) {
    BinprotResponse rsp;
    do {
        BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::GetReplica);
        cmd.setVBucket(Vbid(0));
        cmd.setKey(key);

        rsp = conn.execute(cmd);
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
    EXPECT_TRUE(rsp.isSuccess());
}

std::string cb::test::ClusterTest::createKey(CollectionIDType cid,
                                             const std::string& key) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb(cid);
    std::string ret;
    std::copy(leb.begin(), leb.end(), std::back_inserter(ret));
    ret.append(key);
    return ret;
}

void cb::test::UpgradeTest::SetUpTestCase() {
    // Can't just use the ClusterTest::SetUp as we need to prevent it from
    // creating replication streams so that we can create them as we like to
    // mock upgrade scenarios.
    cluster = Cluster::create(3);
    if (!cluster) {
        std::cerr << "Failed to create the cluster" << std::endl;
        std::exit(EXIT_FAILURE);
    }

    try {
        bucket = cluster->createBucket(
                "default",
                {{"replicas", 2}, {"max_vbuckets", 1}, {"max_num_shards", 1}},
                {},
                false /*No replication*/);
    } catch (const std::runtime_error& error) {
        std::cerr << error.what();
        std::exit(EXIT_FAILURE);
    }
}