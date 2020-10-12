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

#include "test_helpers.h"

#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "item.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>
#include <libcouchstore/couch_db.h>
#include <nlohmann/json.hpp>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_server.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <memory>
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

queued_item makeCommittedItem(StoredDocKey key, std::string value) {
    queued_item qi{new Item(key, 0, 0, value.data(), value.size())};
    return qi;
}

queued_item makeCommittedviaPrepareItem(StoredDocKey key, std::string value) {
    queued_item qi{new Item(key, 0, 0, value.data(), value.size())};
    qi->setCommittedviaPrepareSyncWrite();
    return qi;
}

queued_item makePendingItem(StoredDocKey key,
                            const std::string& value,
                            cb::durability::Requirements reqs) {
    queued_item qi{new Item(key, 0, 0, value.data(), value.size())};
    qi->setPendingSyncWrite(reqs);
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
    auto handle = vbucket.lockCollections(qi->getKey());
    return vbucket.checkpointManager->queueDirty(vbucket,
                                                 qi,
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
    std::string filename = dbDir + "/" + std::to_string(vbucket.get()) +
                           ".couch." + std::to_string(revision);
    Db* handle;
    couchstore_error_t err = couchstore_open_db(
            filename.c_str(), COUCHSTORE_OPEN_FLAG_CREATE, &handle);

    ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to open new database";

    nlohmann::json vbstateJson;
    vbstateJson["state"] = "active";
    vbstateJson["checkpoint_id"] = "1";
    vbstateJson["max_deleted_seqno"] = "0";
    vbstateJson["snap_start"] = "0";
    vbstateJson["snap_end"] = "0";
    vbstateJson["max_cas"] = "12345";

    // hlc_epoch added in v5.0.0, but the value could be
    // HlcCasSeqnoUninitialised if the couchstore file was previously upgraded
    // from an older version.
    vbstateJson["hlc_epoch"] = std::to_string(HlcCasSeqnoUninitialised);
    vbstateJson["might_contain_xattrs"] = false;

    if (namespacesSupported) {
        vbstateJson["namespaces_supported"] = true;
    }

    auto str = vbstateJson.dump();

    LocalDoc vbstate;
    vbstate.id.buf = (char*)"_local/vbstate";
    vbstate.id.size = sizeof("_local/vbstate") - 1;
    vbstate.json.buf = const_cast<char*>(str.data());
    vbstate.json.size = str.size();
    vbstate.deleted = 0;

    err = couchstore_save_local_document(handle, &vbstate);
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
