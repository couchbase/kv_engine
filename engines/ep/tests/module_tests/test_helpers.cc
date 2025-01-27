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

#include <boost/algorithm/string/replace.hpp>
#include <folly/portability/GTest.h>
#include <libcouchstore/couch_db.h>
#include <nlohmann/json.hpp>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <platform/timeutils.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/string_utilities.h>
#include <xattr/blob.h>
#include <filesystem>
#include <memory>
#include <regex>
#include <thread>

using namespace std::string_literals;

StoredDocKey makeStoredDocKey(const std::string& string, CollectionID ns) {
    return {string, ns};
}

DiskDocKey makeDiskDocKey(const std::string& string,
                          bool prepare,
                          CollectionID ns) {
    StoredDocKey sdk(string, ns);
    return DiskDocKey(sdk, prepare);
}

Item make_item(Vbid vbid,
               const DocKeyView& key,
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
                                           const DocKeyView& key,
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
        if (!deflateSnappy(value, output)) {
            throw std::runtime_error("Failed to deflate value");
        }
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

bool queueNewItem(VBucket& vbucket, DocKeyView key) {
    queued_item qi{new Item(key,
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
        if (!deflateSnappy(data, output)) {
            throw std::runtime_error("Failed to deflate value");
        }
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

std::string getProcessUniqueDatabaseName() {
    return fmt::format("unit_tests_db_{}", getpid());
}

void removePathIfExists(const std::string& path) {
    std::filesystem::path p(path);
    if (exists(p)) {
        // try a few times while backing off in case someone else holds
        // the resource open (and thats the reason why we fail to remove
        // the file.
        for (int ii = 0; ii < 20; ++ii) {
            try {
                std::filesystem::remove_all(p);
                return;
            } catch (const std::exception& e) {
                std::cerr << "Failed to remove: " << p.generic_string() << ": "
                          << e.what() << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds{20});
            }
        }

        // Try a final time and this time we'll throw an exception
        std::filesystem::remove_all(p);
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
    std::ranges::replace(ret, ':', ';');

    // Whilst we take the "couchdb" parameter for the backend in
    // configuration.json we actually use couchstore so all of our test
    // parameters are labelled "couchstore" and we need to replace those with
    // "couchdb" to pass into the engine config.
    boost::replace_all(ret, "couchstore", "couchdb");
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

namespace cb::testing::sv {

std::vector<HasValue> hasValueValues(HasValue hasValue) {
    return hasValue == HasValue::Any ? std::vector{HasValue::Yes, HasValue::No}
                                     : std::vector{hasValue};
}

std::vector<Resident> residentValues(Resident resident) {
    return resident == Resident::Any ? std::vector{Resident::Yes, Resident::No}
                                     : std::vector{resident};
}

std::vector<Persisted> persistedValues(Persisted persisted) {
    return persisted == Persisted::Any
                   ? std::vector{Persisted::Yes, Persisted::No}
                   : std::vector{persisted};
}

std::vector<Deleted> deletedValues(Deleted deleted) {
    return deleted == Deleted::Any ? std::vector{Deleted::Yes, Deleted::No}
                                   : std::vector{deleted};
}

std::vector<Expired> expiredValues(Expired expired) {
    return expired == Expired::Any ? std::vector{Expired::Yes, Expired::No}
                                   : std::vector{expired};
}

std::vector<Locked> lockedValues(Locked locked) {
    return locked == Locked::Any ? std::vector{Locked::Yes, Locked::No}
                                 : std::vector{locked};
}

std::vector<protocol_binary_datatype_t> datatypeValues(
        protocol_binary_datatype_t datatype) {
    std::vector<protocol_binary_datatype_t> results;
    for (protocol_binary_datatype_t i = 0; i < cb::mcbp::datatype::highest;
         i++) {
        if ((i & datatype) == i) {
            results.push_back(i);
        }
    }
    return results;
}

std::vector<State> stateValues(State state) {
    std::vector<State> stateValues;
    for (auto flag : {State::Document,
                      State::TempInitial,
                      State::TempDeleted,
                      State::TempNonExistent}) {
        if (static_cast<unsigned>(state) & static_cast<unsigned>(flag)) {
            stateValues.push_back(flag);
        }
    }
    return stateValues;
}

template <typename... Ts>
std::vector<std::tuple<Ts...>> combinations(const std::vector<Ts>&... ts) {
    using namespace ::testing;
    // None of our dependencies provide a Cartesian product function, and it is
    // simpler to re-use the GTest one, given this is test code.
    using Params = std::tuple<Ts...>;
    // The target generator type which we can iterate.
    using Generator = decltype(Range<Params>(std::declval<Params>(),
                                             std::declval<Params>()));
    // Call combine and implicitly convert the result to the above type.
    Generator g = Combine(ValuesIn(ts)...);
    // The iterator returned is not compatible with the std::vector constructor,
    // so we need a loop.
    std::vector<std::tuple<Ts...>> results;
    for (auto v : g) {
        results.emplace_back(v);
    }
    return results;
}

StoredValue::UniquePtr createWithFactory(AbstractStoredValueFactory& factory,
                                         const DocKeyView& key,
                                         State s,
                                         HasValue v,
                                         protocol_binary_datatype_t t,
                                         Resident r,
                                         Persisted p,
                                         Deleted d,
                                         Expired e,
                                         Locked l) {
    Item item(key,
              /*flags*/ 0,
              /*exp*/ 0,
              value_t{});

    switch (s) {
    case State::Document:
        item.setBySeqno(1);
        break;
    case State::TempInitial:
        item.setBySeqno(StoredValue::state_temp_init);
        break;
    case State::TempDeleted:
        item.setBySeqno(StoredValue::state_deleted_key);
        break;
    case State::TempNonExistent:
        item.setBySeqno(StoredValue::state_non_existent_key);
        break;
    default:
        throw std::logic_error("Unexpected invalid state!");
    }

    auto sv = factory(item, {});

    if (s != State::Document) {
        // Temp items have no value.
        Expects(v == HasValue::No);
        Expects(t == PROTOCOL_BINARY_RAW_BYTES);
        // Temp items are always clean.
        Expects(p == Persisted::Yes);
        // Temp items are never resident.
        Expects(r == Resident::No);
        // Temp items are never deleted (the bit).
        Expects(d == Deleted::No);
        Expects(e == Expired::No);
        Expects(l == Locked::No);
        return sv;
    }

    Expects(d != Deleted::Any);
    if (d == Deleted::Yes) {
        sv->markDeleted(DeleteSource::Explicit);
    }

    Expects(v != HasValue::Any);
    if (v == HasValue::Yes) {
        std::string body;
        if (cb::mcbp::datatype::is_xattr(t)) {
            cb::xattr::Blob blob;
            blob.set("attr", "\"string\"");
            body.append(blob.finalize());
        }
        if (cb::mcbp::datatype::is_json(t)) {
            body.append("{}");
        } else {
            body.append("abc");
        }
        if (cb::mcbp::datatype::is_snappy(t)) {
            cb::compression::Buffer deflated;
            Expects(cb::compression::deflateSnappy(body, deflated));
            sv->replaceValue(std::unique_ptr<Blob>(
                    Blob::New(deflated.data(), deflated.size())));
        } else {
            sv->replaceValue(
                    std::unique_ptr<Blob>(Blob::New(body.data(), body.size())));
        }
    } else {
        sv->resetValue();
    }

    Expects(r != Resident::Any);
    if (r == Resident::Yes) {
        Expects(sv->isResident());
    } else {
        // Note: SV without a value can still be resident/non-resident depending
        // on whether they were created by normal op or a meta BGFetch.
        sv->ejectValue();
    }

    Expects(e != Expired::Any);
    if (e == Expired::Yes) {
        // Smallest possible exptime.
        sv->setExptime(1);
    } else {
        sv->setExptime(0);
    }

    Expects(p != Persisted::Any);
    if (p == Persisted::Yes) {
        sv->markClean();
    } else {
        sv->markDirty();
    }

    Expects(l != Locked::Any);
    if (l == Locked::Yes) {
        Expects(d != Deleted::Yes);
        sv->lock(std::numeric_limits<rel_time_t>::max(),
                 std::numeric_limits<uint64_t>::max());
    }

    return sv;
}

StoredValue::UniquePtr create(const DocKeyView& key,
                              State state,
                              HasValue value,
                              protocol_binary_datatype_t datatype,
                              Resident resident,
                              Persisted persisted,
                              Deleted deleted,
                              Expired expired,
                              Locked locked) {
    StoredValueFactory factory;
    return createWithFactory(factory,
                             key,
                             state,
                             value,
                             datatype,
                             resident,
                             persisted,
                             deleted,
                             expired,
                             locked);
}

std::vector<StoredValue::UniquePtr> createAllWithFactory(
        AbstractStoredValueFactory& factory,
        const DocKeyView& key,
        State state,
        HasValue value,
        protocol_binary_datatype_t datatype,
        Resident resident,
        Persisted persisted,
        Deleted deleted,
        Expired expired,
        Locked locked) {
    // Ordered in the order we print them in operator<<.
    const auto c = combinations(datatypeValues(datatype),
                                persistedValues(persisted),
                                deletedValues(deleted),
                                residentValues(resident),
                                lockedValues(locked),
                                expiredValues(expired),
                                hasValueValues(value));
    std::vector<StoredValue::UniquePtr> results;

    for (auto s : stateValues(state)) {
        if (s == State::Document) {
            for (auto [t, p, d, r, l, e, v] : c) {
                if (d == Deleted::Yes && l == Locked::Yes) {
                    // Deletes cannot be locked.
                    continue;
                }
                results.emplace_back(createWithFactory(
                        factory, key, State::Document, v, t, r, p, d, e, l));
            }
        } else {
            // Note: temp items are never deleted (even tempDeleted is not).
            results.emplace_back(createWithFactory(factory,
                                                   key,
                                                   s,
                                                   HasValue::No,
                                                   PROTOCOL_BINARY_RAW_BYTES,
                                                   Resident::No,
                                                   Persisted::Yes,
                                                   Deleted::No,
                                                   Expired::No,
                                                   Locked::No));
        }
    }

    return results;
}

std::vector<StoredValue::UniquePtr> createAll(
        const DocKeyView& key,
        State state,
        HasValue value,
        protocol_binary_datatype_t datatype,
        Resident resident,
        Persisted persisted,
        Deleted deleted,
        Expired expired,
        Locked locked) {
    StoredValueFactory factory;
    return createAllWithFactory(factory,
                                key,
                                state,
                                value,
                                datatype,
                                resident,
                                persisted,
                                deleted,
                                expired,
                                locked);
}

} // namespace cb::testing::sv
