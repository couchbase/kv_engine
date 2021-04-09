/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * kvstore_gen - Test program to generate (couch)kvstore files with
 * interesting properties.
 */

#include "atomic.h"
#include "bucket_logger.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "configuration.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore_config.h"
#include "vbucket_state.h"
#include <logger/logger.h>
#include <programs/engine_testapp/mock_server.h>
#include <spdlog/fmt/fmt.h>

#include <engines/ep/src/vb_commit.h>
#include <memcached/util.h>
#include <random>

using namespace std::string_literals;

std::random_device randomDevice;
std::mt19937_64 randomGenerator(randomDevice());

static std::string makeRandomString(int size) {
    std::uniform_int_distribution<> dis(0, 255);
    std::string result;
    for (int i = 0; i < size; i++) {
        result += dis(randomGenerator);
    }
    return result;
}

static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=true";

int main(int argc, char** argv) {
    if (argc != 6) {
        fmt::print(stderr,
                   "Usage: {} <filename> <total_docs> <doc_size> "
                   "<updates_per_commit> "
                   "<num_commits>\n",
                   argv[0]);
        return EXIT_FAILURE;
    }
    std::string filename = argv[1];
    uint64_t totalDocs;
    uint64_t docSize;
    uint64_t updatesPerCommit;
    uint64_t numCommits;
    if (!safe_strtoull(argv[2], totalDocs)) {
        fmt::print(stderr,
                   "Fatal: Failed to convert {} to number for <total_docs>\n",
                   argv[2]);
        return EXIT_FAILURE;
    }
    if (!safe_strtoull(argv[3], docSize)) {
        fmt::print(stderr,
                   "Fatal: Failed to convert {} to number for <total_docs>\n",
                   argv[3]);
        return EXIT_FAILURE;
    }
    if (!safe_strtoull(argv[4], updatesPerCommit)) {
        fmt::print(stderr,
                   "Fatal: Failed to convert {} to number for "
                   "<updates_per_commit>\n",
                   argv[4]);
        return EXIT_FAILURE;
    }
    if (!safe_strtoull(argv[5], numCommits)) {
        fmt::print(stderr,
                   "Fatal: Failed to convert {} to number for <num_commits>\n",
                   argv[5]);
        return EXIT_FAILURE;
    }

    // Necessary to work without thread-local engine object.
    putenv(allow_no_stats_env);

    // Enable critical-level logging, so user can see any ep-engine errors.
    cb::logger::createConsoleLogger();
    BucketLogger::setLoggerAPI(get_mock_server_api()->log);
    globalBucketLogger->set_level(spdlog::level::level_enum::debug);

    fmt::print(
            "Creating a vBucket with {} initial documents of size {} bytes, "
            "followed by {} "
            "commit batches of {} items each.\n",
            totalDocs,
            docSize,
            numCommits,
            updatesPerCommit);

    // Create a vBucket in an initial active state.
    Configuration config;
    config.parseConfiguration(("dbname="s + filename).c_str(),
                              get_mock_server_api());
    CouchKVStoreConfig kvStoreConfig(config, 1, 0);
    auto kvstore = KVStoreFactory::create(kvStoreConfig);
    vbucket_state state;
    state.transition.state = vbucket_state_active;
    Vbid vbid{0};
    kvstore.rw->snapshotVBucket(vbid, state);

    std::vector<StoredDocKey> keys;
    // Populate vBucket with N documents
    const std::string key = "key";
    kvstore.rw->begin(std::make_unique<TransactionContext>(vbid));
    for (uint64_t i = 0; i < totalDocs; i++) {
        keys.emplace_back(key + std::to_string(i), CollectionID::Default);
        auto value = makeRandomString(docSize);
        queued_item qi{new Item(keys.back(), 0, 0, value.data(), value.size())};
        qi->setBySeqno(i);
        kvstore.rw->set(qi);
    }
    Collections::VB::Manifest manifest{
            std::make_shared<Collections::Manager>()};
    VB::Commit commit(manifest);
    kvstore.rw->commit(commit);

    // Now perform specified number of commits.
    // Shuffle initial keys, then mutate each key in turn (uniform random
    // distribution, arguably worst-case).
    std::shuffle(keys.begin(), keys.end(), randomDevice);

    int updates = 0;
    auto keyIt = keys.begin();
    for (uint64_t c = 0; c < numCommits; c++) {
        kvstore.rw->begin(std::make_unique<TransactionContext>(vbid));
        for (uint64_t u = 0; u < updatesPerCommit; u++) {
            auto value = makeRandomString(docSize);
            queued_item qi{new Item(*keyIt, 0, 0, value.data(), value.size())};
            qi->setBySeqno(totalDocs + updates);
            kvstore.rw->set(qi);

            keyIt++;
            if (keyIt == keys.end()) {
                keyIt = keys.begin();
            }
            updates++;
        }
        VB::Commit commit(manifest);
        kvstore.rw->commit(commit);
    }

    // Teardown
    globalBucketLogger.reset();
}
