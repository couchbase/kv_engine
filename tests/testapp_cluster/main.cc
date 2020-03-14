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
#include "dcp_replicator.h"
#include <event2/thread.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <csignal>
#include <cstdlib>
#include <string>

class BasicClusterTest : public cb::test::ClusterTest {};

TEST_F(BasicClusterTest, GetReplica) {
    auto bucket = cluster->getBucket("default");
    {
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        auto info = conn->store("foo", Vbid(0), "value");
        EXPECT_NE(0, info.cas);
    }

    // make sure that it is replicated to all the nodes in the
    // cluster
    const auto nrep = bucket->getVbucketMap()[0].size() - 1;
    for (std::size_t rep = 0; rep < nrep; ++rep) {
        auto conn = bucket->getConnection(Vbid(0), vbucket_state_replica, rep);
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());

        BinprotResponse rsp;
        do {
            BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::GetReplica);
            cmd.setVBucket(Vbid(0));
            cmd.setKey("foo");

            rsp = conn->execute(cmd);
        } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
        EXPECT_TRUE(rsp.isSuccess());
    }
}

TEST_F(BasicClusterTest, MultiGet) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());

    std::vector<std::pair<const std::string, Vbid>> keys;
    for (int ii = 0; ii < 10; ++ii) {
        keys.emplace_back(std::make_pair("key_" + std::to_string(ii), Vbid(0)));
        if ((ii & 1) == 1) {
            conn->store(keys.back().first,
                        keys.back().second,
                        "value",
                        cb::mcbp::Datatype::Raw);
        }
    }

    // and I want a not my vbucket!
    keys.emplace_back(std::make_pair("NotMyVbucket", Vbid(1)));
    bool nmvb = false;
    int nfound = 0;
    conn->mget(keys,
               [&nfound](std::unique_ptr<Document>&) -> void { ++nfound; },
               [&nmvb](const std::string& key,
                       const cb::mcbp::Response& rsp) -> void {
                   EXPECT_EQ("NotMyVbucket", key);
                   EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, rsp.getStatus());
                   nmvb = true;
               });

    EXPECT_TRUE(nmvb) << "Did not get the NotMyVbucket callback";
    EXPECT_EQ(5, nfound);
}

char isasl_env_var[1024];
int main(int argc, char** argv) {
    setupWindowsDebugCRTAssertHandling();
    cb_initialize_sockets();

#if defined(EVTHREAD_USE_WINDOWS_THREADS_IMPLEMENTED)
    const auto failed = evthread_use_windows_threads() == -1;
#elif defined(EVTHREAD_USE_PTHREADS_IMPLEMENTED)
    const auto failed = evthread_use_pthreads() == -1;
#else
#error "No locking mechanism for libevent available!"
#endif

    if (failed) {
        std::cerr << "Failed to enable libevent locking. Terminating program"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    // We need to set MEMCACHED_UNIT_TESTS to enable the use of
    // the ewouldblock engine..
    static char envvar[80];
    snprintf(envvar, sizeof(envvar), "MEMCACHED_UNIT_TESTS=true");
    putenv(envvar);

    const auto isasl_file_name =
            cb::io::sanitizePath(SOURCE_ROOT "/tests/testapp/cbsaslpw.json");

    // Add the file to the exec environment
    snprintf(isasl_env_var,
             sizeof(isasl_env_var),
             "CBSASL_PWFILE=%s",
             isasl_file_name.c_str());
    putenv(isasl_env_var);

#ifndef WIN32
    if (sigignore(SIGPIPE) == -1) {
        std::cerr << "Fatal: failed to ignore SIGPIPE; sigaction" << std::endl;
        return 1;
    }
#endif
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using UpgradeTest = cb::test::UpgradeTest;
TEST_F(UpgradeTest, ExpiryOpcodeDoesntEnableDeleteV2) {
    // MB-38390: Check that DcpProducers respect the includeDeleteTime flag
    // sent at dcpOpen, and "enable_expiry_opcode" did not erroneously cause the
    // producer to send deletion times.

    // Set up replication from node 0 to node 1 with flags==0, specifically so
    // includeDeleteTime is disabled.
    bucket->setupReplication({{0, 1, false, 0}, {0, 2, false, 0}});

    // store and delete a document on the active
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());
    auto info = conn->store("foo", Vbid(0), "value");
    EXPECT_NE(0, info.cas);
    info = conn->remove("foo", Vbid(0));
    EXPECT_NE(0, info.cas);

    // make sure that the delete is replicated to all replicas
    const auto nrep = bucket->getVbucketMap()[0].size() - 1;
    for (std::size_t rep = 0; rep < nrep; ++rep) {
        conn = bucket->getConnection(Vbid(0), vbucket_state_replica, rep);
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());

        // Wait for persistence of our item
        ObserveInfo observeInfo;
        do {
            observeInfo = conn->observeSeqno(Vbid(0), info.vbucketuuid);
        } while (observeInfo.lastPersistedSeqno != info.seqno);
    }

    // Done! The consumer side dcp_deletion_validator would throw if
    // the producer sent a V2 deletion despite the flag being unset.
}