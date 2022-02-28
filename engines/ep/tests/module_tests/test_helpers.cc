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

#include "test_helpers.h"

#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "item.h"
#include "kvstore/storage_common/storage_common/local_doc_constants.h"
#include "vbucket.h"

#include <boost/filesystem.hpp>
#include <folly/portability/GTest.h>
#include <libcouchstore/couch_db.h>
#include <nlohmann/json.hpp>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_server.h>
#include <string_utilities.h>
#include <xattr/blob.h>

#include <memory>
#include <regex>
#include <thread>

using namespace std::string_literals;

StoredDocKey makeStoredDocKey(const std::string& string, CollectionID ns) {
    return StoredDocKey(string, ns);
}

DiskDocKey makeDiskDocKey(const std::string& string,
                          bool prepare,
                          CollectionID ns) {
    StoredDocKey sdk(string, ns);
    return DiskDocKey(sdk, prepare);
}

Item make_item(Vbid vbid,
               const DocKey& key,
               const std::string& value,
               uint32_t exptime,
               protocol_binary_datatype_t datatype) {
    Item item(key,
              /*flags*/ 0,
              /*exp*/ exptime,
              value.c_str(),
              value.size(),
              datatype);
    item.setVBucketId(vbid);
    return item;
}

queued_item makeCommittedItem(StoredDocKey key, std::string value, Vbid vbid) {
    queued_item qi{new Item(key, 0, 0, value.data(), value.size())};
    qi->setVBucketId(vbid);
    return qi;
}

queued_item makeCommittedviaPrepareItem(StoredDocKey key, std::string value) {
    queued_item qi{new Item(key, 0, 0, value.data(), value.size())};
    qi->setCommittedviaPrepareSyncWrite();
    return qi;
}

queued_item makeDeletedItem(StoredDocKey key) {
    auto qi = make_STRCPtr<Item>(key, 0, 0, value_t{});
    qi->setDeleted();
    return qi;
}

queued_item makePendingItem(StoredDocKey key,
                            const std::string& value,
                            cb::durability::Requirements reqs) {
    queued_item qi{new Item(key, 0, 0, value.data(), value.size())};
    qi->setPendingSyncWrite(reqs);
    return qi;
}

queued_item makeAbortedItem(StoredDocKey key, const std::string& value) {
    queued_item qi{new Item(key, 0, 0, value.data(), value.size())};
    qi->setAbortSyncWrite();
    return qi;
}

std::unique_ptr<Item> makeCompressibleItem(Vbid vbid,
                                           const DocKey& key,
                                           const std::string& body,
                                           protocol_binary_datatype_t datatype,
                                           bool shouldCompress,
                                           bool makeXattrBody) {
    protocol_binary_datatype_t itemDataType = datatype;
    std::string value = body;
    if (makeXattrBody) {
        value = createXattrValue(body, true, false);
        itemDataType |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    if (shouldCompress) {
        cb::compression::Buffer output;
        cb::compression::deflate(
                cb::compression::Algorithm::Snappy, value, output);
        itemDataType |= PROTOCOL_BINARY_DATATYPE_SNAPPY;
        return std::make_unique<Item>(key, /*flags*/0, /*exp*/0,
                                      output.data(), output.size(),
                                      itemDataType);
    }

    return std::make_unique<Item>(key,
                                  0 /*flags*/,
                                  0 /*exp*/,
                                  value.c_str(),
                                  value.length(),
                                  itemDataType,
                                  0 /*cas*/,
                                  -1 /*seqno*/,
                                  vbid);
}

bool queueNewItem(VBucket& vbucket, const std::string& key) {
    queued_item qi{new Item(makeStoredDocKey(key),
                            vbucket.getId(),
                            queue_op::mutation,
                            /*revSeq*/ 0,
                            /*bySeq*/ 0)};
    qi->setQueuedTime();
    auto handle = vbucket.lockCollections(qi->getKey());
    return vbucket.checkpointManager->queueDirty(qi,
                                                 GenerateBySeqno::Yes,
                                                 GenerateCas::Yes,
                                                 /*preLinkDocCtx*/ nullptr);
}

std::chrono::microseconds decayingSleep(std::chrono::microseconds uSeconds) {
    /* Max sleep time is slightly over a second */
    static const std::chrono::microseconds maxSleepTime(0x1 << 20);
    std::this_thread::sleep_for(uSeconds);
    return std::min(uSeconds * 2, maxSleepTime);
}

bool waitForPredicate(const std::function<bool()>& pred,
                      std::chrono::microseconds maxWaitTime) {
    using namespace std::chrono;
    auto deadline = steady_clock::now() + maxWaitTime;
    std::chrono::microseconds sleepTime(128);
    do {
        if (pred()) {
            return true;
        }
        sleepTime = decayingSleep(sleepTime);
    } while (steady_clock::now() < deadline);
    return false;
}

std::string createXattrValue(const std::string& body,
                             bool withSystemKey,
                             bool makeItSnappy) {
    cb::xattr::Blob blob;

    // Add enough XATTRs to be sure we would compress it if required
    blob.set("ABCuser1", R"({"author":"bubba"})");
    blob.set("ABCuser2", R"({"author":"bubba"})");
    blob.set("ABCuser3", R"({"author":"bubba"})");
    blob.set("ABCuser4", R"({"author":"bubba"})");
    blob.set("ABCuser5", R"({"author":"bubba"})");
    blob.set("ABCuser6", R"({"author":"bubba"})");

    if (withSystemKey) {
        blob.set("_sync", R"({"cas":"0xdeadbeefcafefeed"})");
    }
    blob.set("meta", R"({"content-type":"text"})");

    auto xattr_value = blob.finalize();

    // append body to the xattrs and store in data
    std::string data{xattr_value};
    data += body;

    if (makeItSnappy) {
        cb::compression::Buffer output;
        cb::compression::deflate(
                cb::compression::Algorithm::Snappy, data, output);
        return {output.data(), output.size()};
    }
    return data;
}

TimeTraveller::TimeTraveller(int by) : by(by) {
    mock_time_travel(by);
}

TimeTraveller::~TimeTraveller() {
    // restore original timeline.
    mock_time_travel(-by);
}

void rewriteCouchstoreVBState(Vbid vbucket,
                              const std::string& dbDir,
                              int revision,
                              bool namespacesSupported) {
    auto modifyFn = [namespacesSupported](nlohmann::json& vbstateJson) {
        vbstateJson.clear();
        vbstateJson["state"] = "active";
        vbstateJson["checkpoint_id"] = "1";
        vbstateJson["max_deleted_seqno"] = "0";
        vbstateJson["snap_start"] = "0";
        vbstateJson["snap_end"] = "0";
        vbstateJson["max_cas"] = "12345";

        // hlc_epoch added in v5.0.0, but the value could be
        // HlcCasSeqnoUninitialised if the couchstore file was previously
        // upgraded from an older version.
        vbstateJson["hlc_epoch"] = std::to_string(HlcCasSeqnoUninitialised);
        vbstateJson["might_contain_xattrs"] = false;

        if (namespacesSupported) {
            vbstateJson["namespaces_supported"] = true;
        }
    };
    modifyCouchstoreVBState(vbucket, dbDir, revision, modifyFn);
}

void modifyCouchstoreVBState(
        Vbid vbucket,
        const std::string& dbDir,
        int revision,
        std::function<void(nlohmann::json& vbState)> modifyFn) {
    std::string filename = dbDir + "/" + std::to_string(vbucket.get()) +
                           ".couch." + std::to_string(revision);
    Db* handle;
    couchstore_error_t err = couchstore_open_db(
            filename.c_str(), COUCHSTORE_OPEN_FLAG_CREATE, &handle);

    ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to open new database";

    std::string key{LocalDocKey::vbstate};
    LocalDoc* doc = nullptr;
    err = couchstore_open_local_document(handle, key.data(), key.size(), &doc);

    nlohmann::json json;
    // Note: File may be just creates, no vbstate on disk is legal
    if (err != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to open '" << key << "'";
        json = nlohmann::json::parse(
                std::string{doc->json.buf, doc->json.size});
        couchstore_free_local_document(doc);
    }
    modifyFn(json);

    auto str = json.dump();

    LocalDoc newDoc;
    newDoc.id.buf = &key[0];
    newDoc.id.size = key.size();
    newDoc.json.buf = &str[0];
    newDoc.json.size = str.size();
    newDoc.deleted = 0;

    err = couchstore_save_local_document(handle, &newDoc);
    ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to write local document";
    err = couchstore_commit(handle);
    ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to commit";
    err = couchstore_close_file(handle);
    ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to close file";
    err = couchstore_free_db(handle);
    ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to free db";
}

std::string dbnameFromCurrentGTestInfo() {
#ifdef WIN32
    // On Windows we quickly end up hitting up against the MAX_PATH or 260
    // characters when attempting to uniqify the dbname. The issues include
    // ones deep inside Magma (see MB-38577).
    // As use a simpler (but less descriptive) name - simply use a temporary
    // directory.
    cb::io::mkdirp("ep_engine_ep_unit_tests.db");
    return cb::io::mkdtemp("ep_engine_ep_unit_tests.db/test.XXXXXX");
#else
    auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    Expects(info);
    auto subdir = std::string(info->test_suite_name()) + "_" + info->name();
    // To avoid having arbitrary levels of nested directories, replace any '/'
    // (used to separate components of parameterized tests) with '_'.
    std::replace(begin(subdir), end(subdir), '/', '_');
    return "ep_engine_ep_unit_tests.db/"s + subdir;
#endif
}

void removePathIfExists(const std::string& path) {
    boost::filesystem::path p(path);
    if (exists(p)) {
        // try a few times while backing off in case someone else holds
        // the resource open (and thats the reason why we fail to remove
        // the file.
        for (int ii = 0; ii < 20; ++ii) {
            try {
                boost::filesystem::remove_all(p);
                return;
            } catch (const std::exception& e) {
                std::cerr << "Failed to remove: " << p.generic_string() << ": "
                          << e.what() << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds{20});
            }
        }

        // Try a final time and this time we'll throw an exception
        boost::filesystem::remove_all(p);
    }
}

std::string getBucketType(std::string_view config) {
    if (config.find("persistent") != std::string::npos) {
        return "persistent";
    }
    return "ephemeral";
}

std::string generateBucketTypeConfig(std::string_view config) {
    std::string ret = "bucket_type=";
    if (config.find("persistent") != std::string::npos) {
        ret += "persistent;";
        ret += generateBackendConfig(config);
    } else {
        ret += config;
    }

    return ret;
}

std::string sanitizeTestParamConfigString(std::string_view config) {
    std::string ret(config);
    // GTest does not like ';' characters in test parameters so we use ':' as a
    // placeholder and replace it here.
    std::replace(ret.begin(), ret.end(), ':', ';');

    // Whilst we take the "couchdb" parameter for the backend in
    // configuration.json we actually use couchstore so all of our test
    // parameters are labelled "couchstore" and we need to replace those with
    // "couchdb" to pass into the engine config.
    ret = std::regex_replace(ret, std::regex("couchstore"), "couchdb");
    return ret;
}

std::string generateBackendConfig(std::string_view config) {
    std::string ret = "backend=";
    if (config.find("persistent") == std::string::npos) {
        throw std::invalid_argument(
                "Not a persistent bucket, backend not applicable");
    }

    auto backendStart = config.find("_") + 1;
    auto withoutBucketType = config.substr(backendStart);
    auto backendEnd = withoutBucketType.find("_");
    auto backend = withoutBucketType.substr(0, backendEnd);

    if (backend == "couchstore") {
        backend = "couchdb";
    }

    ret += backend;

    if (backend == "nexus") {
        ret += generateNexusConfig(withoutBucketType.substr(backendEnd + 1));
    }

    return ret;
}

std::string generateNexusConfig(std::string_view config) {
    // Nexus variant, need to read some extra config
    std::string configString = ";";

    auto primaryEnd = config.find("_");
    auto primary = config.substr(0, primaryEnd);
    auto secondary = config.substr(primaryEnd + 1);

    if (primary == "couchstore") {
        primary = "couchdb";
    }
    if (secondary == "couchstore") {
        secondary = "couchdb";
    }

    configString += "nexus_primary_backend=" + std::string(primary) + ";";
    configString += "nexus_secondary_backend=" + std::string(secondary);

    return configString;
}
