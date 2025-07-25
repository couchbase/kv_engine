/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "ep_test_apis.h"
#include "ep_testsuite_common.h"
#include <fmt/format.h>
#include <folly/portability/SysStat.h>
#include <memcached/engine_error.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <programs/engine_testapp/mock_cookie.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <list>
#include <mutex>
#include <thread>

#include "mock/mock_dcp.h"

#include <string_utils.h>

using namespace std::string_literals;

CouchstoreFileAccessGuard::CouchstoreFileAccessGuard(
        std::string dbName, CouchstoreFileAccessGuard::Mode mode) {
    /* Make the couchstore files in the db directory unwritable.
     *
     * Note we can't just make the directory itself unwritable as other
     * files (e.g. stats.json) need to be written and we are just testing
     * document write failures here.
     */
    const auto dbFiles = cb::io::findFilesContaining(dbName, ".couch.");
    checkeq(size_t{1},
            dbFiles.size(),
            "Expected to find exactly 1 data file in db directory '"s + dbName +
                    "', found:" + std::to_string(dbFiles.size()));

    filename = dbFiles.front();
    // Save existing permissions
    checkeq(0,
            lstat(filename.c_str(), &originalStat),
            "Failed to read existing permissions for file '"s + filename +
                    "': " + cb_strerror());

#ifdef WIN32
    /**
     * We can't use chmod to remove the ability of the process to read the
     * couchstore file, so we must use an alternative. Windows does provide a
     * way to remove read/write access to a file using file security permissions
     * however, this requires involved code to check ACL (access control lists)
     * and is overly complex for this test. So instead to prevent couchstore
     * being able to read the data file on disk, create an exclusive handle to
     * it, which will be released in the destructor of this object.
     *
     */
    if (mode == Mode::DenyAll) {
        hFile = CreateFileA(filename.c_str(),
                            GENERIC_WRITE,
                            0 /*exclusive*/,
                            nullptr,
                            OPEN_EXISTING,
                            0,
                            nullptr);
        checkne(INVALID_HANDLE_VALUE,
                hFile,
                ("Failed to exclusively open file:'"s + filename +
                 "': " + cb_strerror()));
        return;
    }
#endif

    const auto perms =
            (mode == Mode::ReadOnly) ? (S_IRUSR | S_IRGRP | S_IROTH) : 0;

    checkeq(0,
            chmod(filename.c_str(), perms),
            "Failed to make file '"s + dbFiles.at(0) +
                    "' read-only: " + cb_strerror());
}

CouchstoreFileAccessGuard::~CouchstoreFileAccessGuard() {
    // Restore permissions to before we changed them.
    checkeq(0,
            chmod(filename.c_str(), originalStat.st_mode),
            "Failed to make restore permissions to file '"s + filename +
                    "': " + cb_strerror());
#ifdef WIN32
    if (hFile) {
        checkeq(TRUE,
                CloseHandle(hFile),
                "Unable to close handle to file:"s + filename);
    }
#endif
}

// Due to the limitations of the add_stats callback (essentially we cannot pass
// a context into it) we instead have a single, global `vals` map. The
// vals_mutex is to ensure serialised modifications to this data structure.
std::mutex vals_mutex;
statistic_map vals;

// get_stat and get_histo_stat can only be called one at a time as they use
// the three global variables (requested_stat_name, actual_stat_value and
// histogram_stat_int_value).  Therefore the two functions need to acquire a
// lock and keep it for the whole function duration.

// The requested_stat_name and actual_stat_value are used in an optimized
// add_stats callback (add_individual_stat) which checks for one stat
// (and hence doesn't have to keep a map of all of them).
struct {
    std::mutex mutex;
    std::string requested_stat_name;
    std::string actual_stat_value;
} get_stat_context;

bool dump_stats = false;
std::atomic<cb::mcbp::Status> last_status(cb::mcbp::Status::Success);
std::string last_key;
std::string last_body;
std::string last_ext;
std::atomic<uint64_t> last_cas(0);
std::atomic<uint8_t> last_datatype(0x00);
ItemMetaData last_meta;
std::atomic<uint64_t> last_uuid(0);
std::atomic<uint64_t> last_seqno(0);

void encodeExt(char* buffer, uint32_t val, size_t offset = 0);
void encodeWithMetaExt(char *buffer, ItemMetaData *meta);

void decayingSleep(std::chrono::microseconds* sleepTime) {
    static const std::chrono::microseconds maxSleepTime{500000};
    std::this_thread::sleep_for(std::chrono::microseconds(*sleepTime));
    *sleepTime = std::min((*sleepTime) * 2, maxSleepTime);
}

bool add_response(std::string_view key,
                  std::string_view extras,
                  std::string_view body,
                  ValueIsJson json,
                  cb::mcbp::Status status,
                  uint64_t cas,
                  CookieIface& cookie) {
    (void)cookie;
    static std::mutex m;
    std::lock_guard<std::mutex> lg(m);
    last_status.store(status);
    last_body.assign(body.data(), body.size());
    last_ext.assign(extras.data(), extras.size());
    last_key.assign(key.data(), key.size());
    last_cas.store(cas);
    last_datatype.store(json == ValueIsJson::Yes ? PROTOCOL_BINARY_DATATYPE_JSON
                                                 : PROTOCOL_BINARY_RAW_BYTES);
    return true;
}

void add_response_set_del_meta(std::string_view key,
                               std::string_view extras,
                               std::string_view body,
                               ValueIsJson json,
                               cb::mcbp::Status status,
                               uint64_t cas,
                               CookieIface& cookie) {
    if (!extras.empty()) {
        const auto* ext_bytes = reinterpret_cast<const uint8_t*>(extras.data());
        uint64_t vb_uuid;
        uint64_t seqno;
        memcpy(&vb_uuid, ext_bytes, 8);
        memcpy(&seqno, ext_bytes + 8, 8);
        last_uuid.store(ntohll(vb_uuid));
        last_seqno.store(ntohll(seqno));
    }

    add_response(key, extras, body, json, status, cas, cookie);
}

void add_response_ret_meta(std::string_view key,
                           std::string_view extras,
                           std::string_view body,
                           ValueIsJson json,
                           cb::mcbp::Status status,
                           uint64_t cas,
                           CookieIface& cookie) {
    if (extras.size() == 16) {
        const auto* ext_bytes = reinterpret_cast<const uint8_t*>(extras.data());
        memcpy(&last_meta.flags, ext_bytes, 4);

        if (folly::kIsLittleEndian) {
            memcpy(&last_meta.exptime, ext_bytes + 4, 4);
        } else {
            memcpy(reinterpret_cast<char*>(&last_meta.exptime) + 4, ext_bytes + 4, 4);
        }

        last_meta.exptime = ntohl(last_meta.exptime);
        uint64_t revId = 0;
        memcpy(&revId, ext_bytes + 8, 8);
        last_meta.revSeqno = ntohll(revId);
        last_meta.cas = cas;
    }
    add_response(key, extras, body, json, status, cas, cookie);
}

void add_stats(std::string_view key, std::string_view value, CookieIface&) {
    std::string k(key.data(), key.size());
    std::string v(value.data(), value.size());

    if (dump_stats) {
        std::cout << "stat[" << k << "] = " << v << std::endl;
    }

    std::lock_guard<std::mutex> lh(vals_mutex);
    vals[k] = v;
}

/* Callback passed to engine interface `get_stats`, used by get_int_stat and
 * friends to lookup a specific stat. If `key` matches the requested key name,
 * then record its value in actual_stat_value.
 */
static void add_individual_stat(std::string_view key,
                                std::string_view value,
                                CookieIface&) {
    if (get_stat_context.actual_stat_value.empty() &&
        get_stat_context.requested_stat_name.compare(
                0,
                get_stat_context.requested_stat_name.size(),
                key.data(),
                key.size()) == 0) {
        get_stat_context.actual_stat_value =
                std::string(value.data(), value.size());
    }
}

void encodeExt(char* buffer, uint32_t val, size_t offset) {
    val = htonl(val);
    memcpy(buffer + offset, (char*)&val, sizeof(val));
}

void encodeWithMetaExt(char* buffer,
                       uint64_t cas,
                       uint64_t revSeqno,
                       uint32_t flags,
                       uint32_t exp) {
    memcpy(buffer, (char*)&flags, sizeof(flags));
    memcpy(buffer + 4, (char*)&exp, sizeof(exp));
    memcpy(buffer + 8, (char*)&revSeqno, sizeof(revSeqno));
    memcpy(buffer + 16, (char*)&cas, sizeof(cas));
}

void encodeWithMetaExt(char* buffer, RawItemMetaData* meta) {
    uint32_t flags = meta->flags;
    uint32_t exp = htonl(meta->exptime);
    uint64_t seqno = htonll(meta->revSeqno);
    uint64_t cas = htonll(meta->cas);
    encodeWithMetaExt(buffer, cas, seqno, flags, exp);
}

void encodeWithMetaExt(char* buffer, ItemMetaData* meta) {
    uint32_t flags = meta->flags;
    uint32_t exp = htonl(meta->exptime);
    uint64_t seqno = htonll(meta->revSeqno);
    uint64_t cas = htonll(meta->cas);
    encodeWithMetaExt(buffer, cas, seqno, flags, exp);
}

cb::engine_errc del(EngineIface* h,
                    std::string_view key,
                    uint64_t cas,
                    Vbid vbucket,
                    CookieIface* cookie) {
    mutation_descr_t mut_info{};
    return del(h, key, &cas, vbucket, cookie, &mut_info);
}

cb::engine_errc del(EngineIface* h,
                    std::string_view key,
                    uint64_t* cas,
                    Vbid vbucket,
                    CookieIface* cookie,
                    mutation_descr_t* mut_info) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        create_cookie = true;
    }

    auto ret = h->remove(*cookie,
                         DocKeyView(key, DocKeyEncodesCollectionId::No),
                         *cas,
                         vbucket,
                         {},
                         *mut_info);
    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return ret;
}

/** Simplified version of store for handling the common case of performing
 * a delete with a value.
 */
cb::engine_errc delete_with_value(EngineIface* h,
                                  CookieIface* cookie,
                                  uint64_t cas,
                                  std::string_view key,
                                  std::string_view value,
                                  cb::mcbp::Datatype datatype) {
    auto ret = storeCasVb11(h,
                            cookie,
                            StoreSemantics::Set,
                            key,
                            value,
                            9258,
                            cas,
                            Vbid(0),
                            /*exp*/ 0,
                            uint8_t(datatype),
                            DocumentState::Deleted);
    wait_for_flusher_to_settle(h);

    return ret.first;
}

cb::engine_errc del_with_meta(EngineIface* h,
                              std::string_view key,
                              const Vbid vb,
                              ItemMetaData* itemMeta,
                              uint64_t cas_for_delete,
                              uint32_t options,
                              CookieIface* cookie,
                              const std::vector<char>& nmeta,
                              protocol_binary_datatype_t datatype,
                              const std::vector<char>& value) {
    RawItemMetaData meta{itemMeta->cas,
                         itemMeta->revSeqno,
                         itemMeta->flags,
                         itemMeta->exptime};
    return del_with_meta(h,
                         key,
                         vb,
                         &meta,
                         cas_for_delete,
                         options,
                         cookie,
                         nmeta,
                         datatype,
                         value);
}

cb::engine_errc del_with_meta(EngineIface* h,
                              std::string_view key,
                              const Vbid vb,
                              RawItemMetaData* itemMeta,
                              uint64_t cas_for_delete,
                              uint32_t options,
                              CookieIface* cookie,
                              const std::vector<char>& nmeta,
                              protocol_binary_datatype_t datatype,
                              const std::vector<char>& value) {
    size_t blen = 24;
    std::array<char, 30> ext;

    encodeWithMetaExt(ext.data(), itemMeta);

    if (options) {
        uint32_t optionsSwapped = htonl(options);
        memcpy(ext.data() + blen,
               (char*)&optionsSwapped,
               sizeof(optionsSwapped));
        blen += sizeof(uint32_t);
    }

    if (!nmeta.empty()) {
        uint16_t nmetaSize = htons(nmeta.size());
        memcpy(ext.data() + blen, (char*)&nmetaSize, sizeof(nmetaSize));
        blen += sizeof(uint16_t);
    }

    auto pkt = createPacket(cb::mcbp::ClientOpcode::DelWithMeta,
                            vb,
                            cas_for_delete,
                            {ext.data(), blen},
                            key,
                            {value.data(), value.size()},
                            datatype,
                            {nmeta.data(), nmeta.size()});

    std::unique_ptr<MockCookie> cookieHolder;
    if (cookie == nullptr) {
        cookieHolder = std::make_unique<MockCookie>();
        cookie = cookieHolder.get();
    }

    return h->unknown_command(*cookie, *pkt, add_response_set_del_meta);
}

void evict_key(EngineIface* h,
               std::string_view key,
               Vbid vbucketId,
               const char* msg,
               bool expectError) {
    int nonResidentItems = get_int_stat(h, "ep_num_non_resident");
    int numEjectedItems = get_int_stat(h, "ep_num_value_ejects");

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    const auto status = h->evict_key(
            *cookie, DocKeyView{key, DocKeyEncodesCollectionId::No}, vbucketId);

    if (expectError) {
        checkeq(cb::engine_errc::key_already_exists,
                status,
                "evict_key: expected KEY_EEXISTS when evicting key");
    } else {
        if (cookie->getErrorContext() != "Already ejected.") {
            nonResidentItems++;
            numEjectedItems++;
        }
        checkeq(cb::engine_errc::success,
                status,
                "evict_key: expected SUCCESS when evicting key.");
    }

    checkeq(nonResidentItems,
            get_int_stat(h, "ep_num_non_resident"),
            "Incorrect number of non-resident items");
    checkeq(numEjectedItems,
            get_int_stat(h, "ep_num_value_ejects"),
            "Incorrect number of ejected items");

    if (msg != nullptr && cookie->getErrorContext() != msg) {
        fmt::print(stderr,
                   "Expected evict to return '{}', but it returned '{}'\n",
                   msg,
                   cookie->getErrorContext());
        abort();
    }
}

cb::engine_errc seqnoPersistence(EngineIface* h,
                                 CookieIface& cookie,
                                 Vbid vbucket,
                                 uint64_t seqno) {
    return h->wait_for_seqno_persistence(cookie, seqno, vbucket);
}

cb::EngineErrorItemPair gat(EngineIface* h,
                            std::string_view key,
                            Vbid vb,
                            uint32_t exp) {
    auto* cookie = testHarness->create_cookie(h);
    auto ret = h->get_and_touch(*cookie,
                                DocKeyView(key, DocKeyEncodesCollectionId::No),
                                vb,
                                exp,
                                {});
    testHarness->destroy_cookie(cookie);

    if (ret.first == cb::engine_errc::success) {
        item_info info;
        check_expression(h->get_item_info(*ret.second.get(), info),
                         "gat Failed to get item info");

        last_body.assign((const char*)info.value[0].iov_base,
                         info.value[0].iov_len);
    }
    return ret;
}

bool get_item_info(EngineIface* h,
                   item_info* info,
                   std::string_view key,
                   Vbid vb) {
    auto ret = get(h, nullptr, key, vb);
    if (ret.first != cb::engine_errc::success) {
        return false;
    }
    if (!h->get_item_info(*ret.second, *info)) {
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }

    return true;
}

cb::EngineErrorItemPair getl(EngineIface* h,
                             CookieIface* cookie,
                             std::string_view key,
                             Vbid vb,
                             uint32_t lock_timeout) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        create_cookie = true;
    }
    auto ret = h->get_locked(*cookie,
                             DocKeyView(key, DocKeyEncodesCollectionId::No),
                             vb,
                             std::chrono::seconds{lock_timeout});
    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return ret;
}

bool get_meta(EngineIface* h, std::string_view key, CookieIface* cookie) {
    cb::EngineErrorMetadataPair out;

    return get_meta(h, key, out, cookie);
}

bool get_meta(EngineIface* h,
              std::string_view key,
              cb::EngineErrorMetadataPair& out,
              CookieIface* cookie) {
    DocKeyView docKey(key, DocKeyEncodesCollectionId::No);
    bool cookie_create = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        cookie_create = true;
    }

    out = h->get_meta(*cookie, docKey, Vbid(0));

    if (cookie_create) {
        testHarness->destroy_cookie(cookie);
    }

    return out.first == cb::engine_errc::success;
}

cb::engine_errc observe(EngineIface* h,
                        std::string key,
                        Vbid vb,
                        std::function<void(uint8_t, uint64_t)> callback) {
    auto cookie = std::make_unique<MockCookie>();
    uint64_t hint;
    return h->observe(*cookie,
                      DocKeyView{key, DocKeyEncodesCollectionId::No},
                      vb,
                      callback,
                      hint);
}

cb::engine_errc observe_seqno(EngineIface* h, Vbid vb_id, uint64_t uuid) {
    uint64_t vb_uuid = htonll(uuid);
    std::stringstream data;
    data.write((char *) &vb_uuid, sizeof(uint64_t));

    auto request = createPacket(
            cb::mcbp::ClientOpcode::ObserveSeqno, vb_id, 0, {}, {}, data.str());
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    return h->unknown_command(*cookie, *request, add_response);
}

cb::engine_errc set_param(EngineIface* h,
                          EngineParamCategory paramtype,
                          const char* param,
                          const char* val,
                          Vbid vb) {
    MockCookie cookie;
    return h->setParameter(cookie, paramtype, param, val, vb);
}

bool set_vbucket_state(EngineIface* h,
                       Vbid vb,
                       vbucket_state_t state,
                       std::string_view meta) {
    MockCookie cookie;
    cb::engine_errc ret;
    if (meta.empty()) {
        ret = h->setVBucket(
                cookie, vb, cb::mcbp::cas::Wildcard, state, nullptr);
    } else {
        auto json = nlohmann::json::parse(meta);
        ret = h->setVBucket(cookie, vb, cb::mcbp::cas::Wildcard, state, &json);
    }

    return ret == cb::engine_errc::success;
}

bool get_all_vb_seqnos(EngineIface* h,
                       std::optional<RequestedVBState> state,
                       CookieIface* cookie,
                       std::optional<CollectionIDType> collection,
                       cb::engine_errc expectedStatus) {
    unique_request_ptr pkt;

    if (collection) {
        if (!state) {
            // Do the same check so we can print for the user...
            checkeq(state.has_value(),
                    true,
                    "State must be set when "
                    "collection is specified");
            return false;
        }

        std::array<char, sizeof(vbucket_state_t) + sizeof(CollectionIDType)>
                ext;
        encodeExt(ext.data(), static_cast<uint32_t>(*state));
        encodeExt(ext.data(), *collection, sizeof(vbucket_state_t));
        pkt = createPacket(cb::mcbp::ClientOpcode::GetAllVbSeqnos,
                           Vbid(0),
                           0,
                           {ext.data(), ext.size()});
    } else if (state) {
        std::array<char, sizeof(vbucket_state_t)> ext;
        encodeExt(ext.data(), static_cast<uint32_t>(*state));
        pkt = createPacket(cb::mcbp::ClientOpcode::GetAllVbSeqnos,
                           Vbid(0),
                           0,
                           std::string_view{ext.data(), ext.size()});
    } else {
        pkt = createPacket(cb::mcbp::ClientOpcode::GetAllVbSeqnos);
    }

    checkeq(expectedStatus,
            h->unknown_command(*cookie, *pkt, add_response),
            "Error in getting all vb info");

    return last_status == cb::mcbp::Status::Success;
}

void verify_all_vb_seqnos(EngineIface* h,
                          int vb_start,
                          int vb_end,
                          std::optional<CollectionID> cid) {
    const int per_vb_resp_size = sizeof(uint16_t) + sizeof(uint64_t);
    const int high_seqno_offset = sizeof(uint16_t);

    /* Check if the total response length is as expected. We expect 10 bytes
     (2 for vb_id + 8 for seqno) */
    checkeq((vb_end - vb_start + 1) * per_vb_resp_size,
            static_cast<int>(last_body.size()),
            "Failed to get all vb info.");
    /* Check if the contents are correct */
    for (int i = 0; i < (vb_end - vb_start + 1); i++) {
        /* Check for correct vb_id */
        checkeq(static_cast<const uint16_t>(vb_start + i),
                ntohs(*(reinterpret_cast<const uint16_t*>(last_body.data() +
                                                          per_vb_resp_size*i))),
              "vb_id mismatch");

        uint64_t high_seqno_vb;
        if (cid) {
            // Get high seqno for the collection in the vBucket
            std::string vb_stat_seqno("vb_" + std::to_string(vb_start + i) +
                                      ":" + cid->to_string() + ":high_seqno");
            high_seqno_vb = get_ull_stat(
                    h, vb_stat_seqno.c_str(), "collections-details");
        } else {
            // Get high seqno for the vBucket
            std::string vb_stat_seqno("vb_" + std::to_string(vb_start + i) +
                                      ":high_seqno");
            high_seqno_vb =
                    get_ull_stat(h, vb_stat_seqno.c_str(), "vbucket-seqno");
        }

        checkeq(high_seqno_vb,
                ntohll(*(reinterpret_cast<const uint64_t*>(last_body.data() +
                                                           per_vb_resp_size*i +
                                                           high_seqno_offset))),
                "high_seqno mismatch");
    }
}

static cb::engine_errc store_with_meta(EngineIface* h,
                                       cb::mcbp::ClientOpcode cmd,
                                       std::string_view key,
                                       std::string_view value,
                                       const Vbid vb,
                                       ItemMetaData* itemMeta,
                                       uint64_t cas_for_store,
                                       uint32_t options,
                                       uint8_t datatype,
                                       CookieIface* cookie,
                                       const std::vector<char>& nmeta) {
    size_t blen = 24;
    std::array<char, 30> ext;
    std::unique_ptr<ExtendedMetaData> emd;

    encodeWithMetaExt(ext.data(), itemMeta);

    if (options) {
        uint32_t optionsSwapped = htonl(options);
        memcpy(ext.data() + blen,
               (char*)&optionsSwapped,
               sizeof(optionsSwapped));
        blen += sizeof(uint32_t);
    }

    if (!nmeta.empty()) {
        uint16_t nmetaSize = htons(nmeta.size());
        memcpy(ext.data() + blen, (char*)&nmetaSize, sizeof(nmetaSize));
        blen += sizeof(uint16_t);
    }

    auto request = createPacket(cmd,
                                vb,
                                cas_for_store,
                                {ext.data(), blen},
                                key,
                                value,
                                datatype,
                                {nmeta.data(), nmeta.size()});

    std::unique_ptr<MockCookie> cookieHolder;
    if (cookie == nullptr) {
        cookieHolder = std::make_unique<MockCookie>();
        cookie = cookieHolder.get();
    }

    return h->unknown_command(*cookie, *request, add_response_set_del_meta);
}

cb::engine_errc set_with_meta(EngineIface* h,
                              std::string_view key,
                              std::string_view value,
                              const Vbid vb,
                              ItemMetaData* itemMeta,
                              uint64_t cas_for_set,
                              uint32_t options,
                              uint8_t datatype,
                              CookieIface* cookie,
                              const std::vector<char>& nmeta) {
    return store_with_meta(h,
                           cb::mcbp::ClientOpcode::SetWithMeta,
                           key,
                           value,
                           vb,
                           itemMeta,
                           cas_for_set,
                           options,
                           datatype,
                           cookie,
                           nmeta);
}

cb::engine_errc add_with_meta(EngineIface* h,
                              std::string_view key,
                              std::string_view value,
                              const Vbid vb,
                              ItemMetaData* itemMeta,
                              uint64_t cas_for_add,
                              uint32_t options,
                              uint8_t datatype,
                              CookieIface* cookie,
                              const std::vector<char>& nmeta) {
    return store_with_meta(h,
                           cb::mcbp::ClientOpcode::AddWithMeta,
                           key,
                           value,
                           vb,
                           itemMeta,
                           cas_for_add,
                           options,
                           datatype,
                           cookie,
                           nmeta);
}

static cb::engine_errc return_meta(EngineIface* h,
                                   std::string_view key,
                                   std::string_view value,
                                   const Vbid vb,
                                   const uint64_t cas,
                                   const uint32_t flags,
                                   const uint32_t exp,
                                   cb::mcbp::request::ReturnMetaType type,
                                   uint8_t datatype,
                                   CookieIface* cookie) {
    cb::mcbp::request::ReturnMetaPayload meta;
    meta.setMutationType(type);
    meta.setFlags(flags);
    meta.setExpiration(exp);

    std::unique_ptr<MockCookie> cookieHolder;
    if (cookie == nullptr) {
        cookieHolder = std::make_unique<MockCookie>();
        cookie = cookieHolder.get();
    }

    auto pkt =
            createPacket(cb::mcbp::ClientOpcode::ReturnMeta,
                         vb,
                         cas,
                         {reinterpret_cast<const char*>(&meta), sizeof(meta)},
                         key,
                         value,
                         datatype);
    return h->unknown_command(*cookie, *pkt, add_response_ret_meta);
}

cb::engine_errc set_ret_meta(EngineIface* h,
                             std::string_view key,
                             std::string_view value,
                             const Vbid vb,
                             const uint64_t cas,
                             const uint32_t flags,
                             const uint32_t exp,
                             uint8_t datatype,
                             CookieIface* cookie) {
    return return_meta(h,
                       key,
                       value,
                       vb,
                       cas,
                       flags,
                       exp,
                       cb::mcbp::request::ReturnMetaType::Set,
                       datatype,
                       cookie);
}

cb::engine_errc add_ret_meta(EngineIface* h,
                             std::string_view key,
                             std::string_view value,
                             const Vbid vb,
                             const uint64_t cas,
                             const uint32_t flags,
                             const uint32_t exp,
                             uint8_t datatype,
                             CookieIface* cookie) {
    return return_meta(h,
                       key,
                       value,
                       vb,
                       cas,
                       flags,
                       exp,
                       cb::mcbp::request::ReturnMetaType::Add,
                       datatype,
                       cookie);
}

cb::engine_errc del_ret_meta(EngineIface* h,
                             std::string_view key,
                             const Vbid vb,
                             const uint64_t cas,
                             CookieIface* cookie) {
    return return_meta(h,
                       key,
                       {},
                       vb,
                       cas,
                       0,
                       0,
                       cb::mcbp::request::ReturnMetaType::Del,
                       0x00,
                       cookie);
}

void disable_traffic(EngineIface* h) {
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    checkeq(cb::engine_errc::success,
            h->set_traffic_control_mode(*cookie, TrafficControlMode::Disabled),
            "Failed to disable traffic in the engine");
}

void enable_traffic(EngineIface* h) {
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    checkeq(cb::engine_errc::success,
            h->set_traffic_control_mode(*cookie, TrafficControlMode::Enabled),
            "Failed to enable traffic in the engine");
}

void start_persistence(EngineIface* h) {
    if (!isPersistentBucket(h)) {
        // Nothing to do for non-persistent buckets
        return;
    }

    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    checkeq(cb::engine_errc::success,
            h->start_persistence(*cookie),
            "Failed to start persistence.");
}

void stop_persistence(EngineIface* h) {
    if (!isPersistentBucket(h)) {
        // Nothing to do for non-persistent buckets
        return;
    }

    std::chrono::microseconds sleepTime{128};
    while (true) {
        if (get_str_stat(h, "ep_flusher_state", nullptr) == "running") {
            break;
        }
        decayingSleep(&sleepTime);
    }
    std::unique_ptr<MockCookie> cookie = std::make_unique<MockCookie>();
    checkeq(cb::engine_errc::success,
            h->stop_persistence(*cookie),
            "Failed to stop persistence.");
}

cb::engine_errc store(
        EngineIface* h,
        CookieIface* cookie,
        StoreSemantics op,
        std::string_view key,
        std::string_view value,
        ItemIface** outitem,
        uint64_t casIn,
        Vbid vb,
        uint32_t exp,
        uint8_t datatype,
        DocumentState docState,
        const std::optional<cb::durability::Requirements>& durReqs) {
    auto ret = storeCasVb11(h,
                            cookie,
                            op,
                            key,
                            value,
                            9258,
                            casIn,
                            vb,
                            exp,
                            datatype,
                            docState,
                            durReqs);
    if (outitem) {
        *outitem = ret.second.release();
    }
    return ret.first;
}

cb::engine_errc storeCasOut(EngineIface* h,
                            CookieIface* cookie,
                            Vbid vb,
                            std::string_view key,
                            std::string_view value,
                            protocol_binary_datatype_t datatype,
                            ItemIface*& out_item,
                            uint64_t& out_cas,
                            DocumentState docState) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        create_cookie = true;
    }

    auto ret = allocate(h, cookie, key, value.size(), 0, 0, datatype, vb);
    checkeq(cb::engine_errc::success, ret.first, "Allocation failed.");
    item_info info;
    check_expression(h->get_item_info(*ret.second.get(), info),
                     "Unable to get item_info");
    memcpy(info.value[0].iov_base, value.data(), value.size());
    cb::engine_errc res = h->store(*cookie,
                                   *ret.second,
                                   out_cas,
                                   StoreSemantics::Set,
                                   {},
                                   docState,
                                   false);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return res;
}

cb::EngineErrorItemPair storeCasVb11(
        EngineIface* h,
        CookieIface* cookie,
        StoreSemantics op,
        std::string_view key,
        std::string_view value,
        uint32_t flags,
        uint64_t casIn,
        Vbid vb,
        uint32_t exp,
        uint8_t datatype,
        DocumentState docState,
        const std::optional<cb::durability::Requirements>& durReqs) {
    uint64_t cas = 0;

    auto rv = allocate(h, cookie, key, value.size(), flags, exp, datatype, vb);
    if (rv.first != cb::engine_errc::success) {
        return rv;
    }
    item_info info;
    if (!h->get_item_info(*rv.second, info)) {
        abort();
    }

    cb_assert(info.value[0].iov_len == value.size());
    std::ranges::copy(value, reinterpret_cast<char*>(info.value[0].iov_base));
    rv.second->setCas(casIn);

    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        create_cookie = true;
    }

    auto storeRet =
            h->store(*cookie, *rv.second, cas, op, durReqs, docState, false);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }

    return {storeRet, std::move(rv.second)};
}

cb::engine_errc replace(EngineIface* h,
                        CookieIface* cookie,
                        std::string_view key,
                        std::string_view value,
                        uint32_t flags,
                        Vbid vb) {
    Expects(cookie);

    auto [status, item] =
            allocate(h, cookie, key, value.size(), flags, 0, 0, vb);
    if (status != cb::engine_errc::success) {
        return status;
    }

    item->setCas(0);
    std::ranges::copy(value, item->getValueBuffer().begin());

    // A predicate that allows the replacement.
    // This simulates the behaviour of replace when the doc being updated does
    // not contain any xattr, and the vbucket that owns the doc has surely never
    // seen a doc with xattr. Which means that we do not need any pre-fetch for
    // preserving xattrs, the replacement can just proceed.
    const cb::StoreIfPredicate predicate = [](const std::optional<item_info>&,
                                              cb::vbucket_info) {
        return cb::StoreIfStatus::Continue;
    };

    return h->store_if(*cookie,
                       *item,
                       cb::mcbp::cas::Wildcard,
                       StoreSemantics::Replace,
                       predicate,
                       {},
                       DocumentState::Alive,
                       false)
            .first;
}

cb::engine_errc touch(EngineIface* h,
                      std::string_view key,
                      Vbid vb,
                      uint32_t exp) {
    auto* cookie = testHarness->create_cookie(h);
    auto result =
            h->get_and_touch(*cookie,
                             DocKeyView(key, DocKeyEncodesCollectionId::No),
                             vb,
                             exp,
                             {});
    testHarness->destroy_cookie(cookie);

    // Update the global cas value (used by some tests)
    if (result.first == cb::engine_errc::success) {
        item_info info{};
        check_expression(h->get_item_info(*result.second.get(), info),
                         "Failed to get item info");
        last_cas.store(info.cas);
    }

    return result.first;
}

cb::engine_errc unl(EngineIface* h,
                    CookieIface* cookie,
                    std::string_view key,
                    Vbid vb,
                    uint64_t cas) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        create_cookie = true;
    }
    auto ret = h->unlock(
            *cookie, DocKeyView(key, DocKeyEncodesCollectionId::No), vb, cas);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }
    return ret;
}

void compact_db(EngineIface* h,
                const Vbid db_file_id,
                const uint64_t purge_before_ts,
                const uint64_t purge_before_seq,
                const uint8_t drop_deletes) {
    MockCookie cookie;
    auto ret = h->compactDatabase(cookie,
                                  db_file_id,
                                  purge_before_ts,
                                  purge_before_seq,
                                  drop_deletes,
                                  {});
    if (ret == cb::engine_errc::not_supported) {
        // Ephemeral, couchdb and magma buckets can
        // return cb::engine_errc::not_supported.  This method is called
        // from a lot of test cases we run. Lets remap the error code to
        // success. Note: Ephemeral buckets use couchdb as backend.
        ret = cb::engine_errc::success;
    }
    checkeq(cb::engine_errc::success, ret, "Failed to request compact vbucket");
}

cb::engine_errc vbucketDelete(EngineIface* h, Vbid vb, const char* args) {
    MockCookie cookie;
    return h->deleteVBucket(cookie, vb, args && strcmp(args, "async=0") == 0);
}

cb::engine_errc verify_key(EngineIface* h, std::string_view key, Vbid vbucket) {
    auto rv = get(h, nullptr, key, vbucket);
    return rv.first;
}

std::pair<cb::engine_errc, std::string> get_value(EngineIface* h,
                                                  CookieIface* cookie,
                                                  std::string_view key,
                                                  Vbid vbucket,
                                                  DocStateFilter state) {
    auto rv = get(h, cookie, key, vbucket, state);
    if (rv.first != cb::engine_errc::success) {
        return {rv.first, ""};
    }
    item_info info;
    if (!h->get_item_info(*rv.second, info)) {
        return {cb::engine_errc::failed, ""};
    }
    auto value = std::string(reinterpret_cast<char*>(info.value[0].iov_base),
                             info.value[0].iov_len);
    return make_pair(rv.first, value);
}

cb::engine_errc get_stats_wrapper(EngineIface* h,
                                  CookieIface& cookie,
                                  std::string_view key,
                                  std::string_view value,
                                  const AddStatFn& add_stat) {
    cb::engine_errc err;
    do {
        err = h->get_stats(cookie, key, value, add_stat);
    } while (err == cb::engine_errc::throttled);
    return err;
}

bool verify_vbucket_missing(EngineIface* h, Vbid vb) {
    const auto vbstr = "vb_" + std::to_string(vb.get());

    // Try up to three times to verify the bucket is missing.  Bucket
    // state changes are async.
    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        vals.clear();
    }

    auto* cookie = testHarness->create_cookie(h);
    checkeq(cb::engine_errc::success,
            get_stats_wrapper(h, *cookie, {}, {}, add_stats),
            "Failed to get stats.");
    testHarness->destroy_cookie(cookie);

    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        if (!vals.contains(vbstr)) {
            return true;
        }

        std::cerr << "Expected bucket missing, got " <<
                vals[vbstr] << std::endl;
    }
    return false;
}

bool verify_vbucket_state(EngineIface* h,
                          Vbid vb,
                          vbucket_state_t expected,
                          bool mute) {
    MockCookie cookie;
    const auto [status, state] = h->getVBucket(cookie, vb);
    if (status != cb::engine_errc::success) {
        if (!mute) {
            fprintf(stderr,
                    "Error code when getting vbucket %s\n",
                    cb::to_string(status).c_str());
        }
        return false;
    }

    return state == expected;
}

void sendDcpAck(EngineIface* h,
                CookieIface* cookie,
                cb::mcbp::ClientOpcode opcode,
                cb::mcbp::Status status,
                uint32_t opaque) {
    cb::mcbp::Response pkt{};
    pkt.setMagic(cb::mcbp::Magic::ClientResponse);
    pkt.setOpcode(opcode);
    pkt.setStatus(status);
    pkt.setOpaque(opaque);

    auto& dcp = dynamic_cast<DcpIface&>(*h);
    checkeq(cb::engine_errc::success,
            dcp.response_handler(*cookie, pkt),
            "Expected success");
}

/* The following set of functions get a given stat as the specified type
 * (int, float, unsigned long, string, bool, etc).
 * If the engine->get_stats() call fails, throws a engine_error exception.
 * If the given statname doesn't exist under the given statname, throws a
 * std::out_of_range exception.
 */
template <>
int get_stat(EngineIface* h,
             const char* statname,
             const char* statkey) {
    return std::stoi(get_str_stat(h, statname, statkey));
}
template <>
uint64_t get_stat(EngineIface* h,
                  const char* statname,
                  const char* statkey) {
    return std::stoull(get_str_stat(h, statname, statkey));
}

template <>
bool get_stat(EngineIface* h,
              const char* statname,
              const char* statkey) {
    return get_str_stat(h, statname, statkey) == "true";
}

template <>
float get_stat(EngineIface* h, const char* statname, const char* statkey) {
    return std::stof(get_str_stat(h, statname, statkey));
}

template <>
std::string get_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey) {
    std::lock_guard<std::mutex> lh(get_stat_context.mutex);

    get_stat_context.requested_stat_name = statname;
    get_stat_context.actual_stat_value.clear();

    auto* cookie = testHarness->create_cookie(h);
    cb::engine_errc err = get_stats_wrapper(
            h,
            *cookie,
            {statkey, statkey == nullptr ? 0 : strlen(statkey)},
            {},
            add_individual_stat);
    testHarness->destroy_cookie(cookie);

    if (err != cb::engine_errc::success) {
        throw cb::engine_error(err, "get_stats failed");
    }

    if (get_stat_context.actual_stat_value.empty()) {
        throw std::out_of_range(std::string("Failed to find requested statname '") +
                                statname + "'");
    }

    // Here we are explictly forcing a copy of the object to work
    // around std::string copy-on-write data-race issues seen on some
    // versions of libstdc++ - see MB-18510 / MB-19688.
    return {get_stat_context.actual_stat_value.begin(),
            get_stat_context.actual_stat_value.end()};
}

/// Backward-compatible functions (encode type name in function name).
int get_int_stat(EngineIface* h,
                 const char* statname,
                 const char* statkey) {
    return get_stat<int>(h, statname, statkey);
}

float get_float_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey) {
    return std::stof(get_str_stat(h, statname, statkey));
}

uint32_t get_ul_stat(EngineIface* h,
                     const char* statname,
                     const char* statkey) {
    return std::stoul(get_str_stat(h, statname, statkey));
}

uint64_t get_ull_stat(EngineIface* h,
                      const char* statname,
                      const char* statkey) {
    return get_stat<uint64_t>(h, statname, statkey);
}

std::string get_str_stat(EngineIface* h,
                         const char* statname,
                         const char* statkey) {
    return get_stat<std::string>(h, statname, statkey);
}

bool get_bool_stat(EngineIface* h,
                   const char* statname,
                   const char* statkey) {
    return cb_stob(get_str_stat(h, statname, statkey));
}

/* Fetches the value for a given statname in the given statkey set.
 * @return te value of statname, or default_value if that statname was not
 * found.
 */
int get_int_stat_or_default(EngineIface* h,
                            int default_value,
                            const char* statname,
                            const char* statkey) {
    try {
        return get_int_stat(h, statname, statkey);
    } catch (std::out_of_range&) {
        return default_value;
    }
}

statistic_map get_all_stats(EngineIface* h, const char* statset) {
    {
        std::lock_guard<std::mutex> lh(vals_mutex);
        vals.clear();
    }
    auto* cookie = testHarness->create_cookie(h);
    auto err = get_stats_wrapper(
            h,
            *cookie,
            {statset, statset == nullptr ? 0 : strlen(statset)},
            {},
            add_stats);
    testHarness->destroy_cookie(cookie);

    if (err != cb::engine_errc::success) {
        throw cb::engine_error(err, "get_stats failed");
    }

    std::lock_guard<std::mutex> lh(vals_mutex);
    return vals;
}

void verify_curr_items(EngineIface* h,
                       int exp,
                       const char* msg) {
    int curr_items = get_int_stat(h, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

void wait_for_stat_to_be_gte(EngineIface* h,
                             const char* stat,
                             int final,
                             const char* stat_key,
                             const std::chrono::seconds max_wait_time_in_secs) {
    std::chrono::microseconds sleepTime{128};
    WaitTimeAccumulator<int> accumulator("to be greater or equal than", stat,
                                         stat_key, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, stat, stat_key);
        if (current >= final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_expired_items_to_be(
        EngineIface* h,
        int final,
        const std::chrono::seconds max_wait_time_in_secs) {
    std::chrono::microseconds sleepTime{128};
    WaitTimeAccumulator<int> accumulator("to be", "expired items",
                                         nullptr, final,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_int_stat(h, "ep_expired_access") +
                       get_int_stat(h, "ep_expired_compactor") +
                       get_int_stat(h, "ep_expired_pager");
        if (current == final) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

void wait_for_memory_usage_below(
        EngineIface* h,
        size_t mem_threshold,
        const std::chrono::seconds max_wait_time_in_secs) {
    std::chrono::microseconds sleepTime{128};
    WaitTimeAccumulator<int> accumulator("to be below", "mem_used", nullptr,
                                         mem_threshold,
                                         max_wait_time_in_secs);
    for (;;) {
        auto current = get_ull_stat(h, "mem_used");
        if (current <= mem_threshold) {
            break;
        }
        accumulator.incrementAndAbortIfLimitReached(current, sleepTime);
        decayingSleep(&sleepTime);
    }
}

bool wait_for_warmup_complete(EngineIface* h) {
    if (!isWarmupEnabled(h)) {
        return true;
    }

    bool waitForSecondary = isSecondaryWarmupEnabled(h);

    std::chrono::microseconds sleepTime{128};
    do {
        try {
            if (get_str_stat(h, "ep_warmup_thread", "warmup") == "complete") {
                if (!waitForSecondary ||
                    (waitForSecondary &&
                     get_str_stat(h, "ep_secondary_warmup_status", "warmup") ==
                             "complete")) {
                    return true;
                }
            }
        } catch (const cb::engine_error&) {
            // If the stat call failed then the warmup stats group no longer
            // exists and hence warmup is complete.
            return true;
        }
        decayingSleep(&sleepTime);
    } while(true);
}

void wait_for_flusher_to_settle(EngineIface* h) {
    wait_for_stat_to_be(h, "ep_queue_size", 0);

    if (!isPersistentBucket(h)) {
        // We don't run flusher in non-persistent buckets
        return;
    }
    // We also need to to wait for any outstanding flushes to disk to
    // complete - specifically so when in full eviction mode we have
    // waited for the item counts in each vBucket to be synced with
    // the number of items on disk. See
    // EPBucket::commit().
    wait_for_stat_to_be(h, "ep_flusher_todo", 0);
}

void wait_for_item_compressor_to_settle(EngineIface* h) {
    int visited_count = get_int_stat(h, "ep_item_compressor_num_visited");

    // We need to wait for at least one more run of the item compressor
    wait_for_stat_to_be_gte(
            h, "ep_item_compressor_num_visited", visited_count + 1);
}

void wait_for_rollback_to_finish(EngineIface* h) {
    std::chrono::microseconds sleepTime{128};
    while (get_int_stat(h, "ep_rollback_count") == 0) {
        decayingSleep(&sleepTime);
    }
}

void wait_for_persisted_value(EngineIface* h,
                              std::string_view key,
                              const char* val,
                              Vbid vbucketId) {
    int commitNum = 0;
    if (isPersistentBucket(h)) {
        commitNum = get_int_stat(h, "ep_commit_num");
    }
    checkeq(cb::engine_errc::success,
            store(h,
                  nullptr,
                  StoreSemantics::Set,
                  key,
                  val,
                  nullptr,
                  0,
                  vbucketId),
            "Failed to store an item.");

    if (isPersistentBucket(h)) {
        // Wait for persistence...
        wait_for_flusher_to_settle(h);
        wait_for_stat_change(h, "ep_commit_num", commitNum);
    }
}

void abort_msg(const char* expr, const char* msg, const char* file, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            file, line, msg, expr);
    throw TestExpectationFailed();
}

/* Helper function to validate the return from store() */
void validate_store_resp(cb::engine_errc ret, int& num_items) {
    switch (ret) {
    case cb::engine_errc::success:
        num_items++;
        break;
    case cb::engine_errc::temporary_failure:
        /* TMPFAIL means we are hitting high memory usage; retry */
        break;
    default:
        check_expression(
                false,
                ("validate_store_resp: Unexpected response from store(): " +
                 cb::to_string(ret))
                        .c_str());
        break;
    }
}

void write_items(EngineIface* h,
                 int num_items,
                 int start_seqno,
                 const char* key_prefix,
                 const char* value,
                 uint32_t expiry,
                 Vbid vb,
                 DocumentState docState) {
    int j = 0;
    while (true) {
        if (j == num_items) {
            break;
        }
        std::string key(key_prefix + std::to_string(j + start_seqno));
        cb::engine_errc ret = store(h,
                                    nullptr,
                                    StoreSemantics::Set,
                                    key,
                                    value,
                                    nullptr,
                                    /*cas*/ 0,
                                    vb,
                                    expiry,
                                    0,
                                    docState);
        validate_store_resp(ret, j);
    }
}

int write_items_upto_mem_perc(EngineIface* h, int mem_thresh_perc) {
    auto maxSize = static_cast<float>(get_int_stat(h, "ep_max_size", "memory"));
    float mem_thresh = static_cast<float>(mem_thresh_perc) / (100.0);
    int num_items = 0;
    while (true) {
        auto memUsed = float(get_int_stat(h, "mem_used", "memory"));
        if (memUsed > (maxSize * mem_thresh)) {
            if (get_stat<uint64_t>(h, "ep_queue_size") == 0) {
                break;
            }

            /* Persist all items written so far. */
            wait_for_flusher_to_settle(h);
            wait_for_stat_to_be(
                    h, "ep_checkpoint_memory_pending_destruction", 0);
            continue;
        }
        const auto ret = store(h,
                               nullptr,
                               StoreSemantics::Set,
                               fmt::format("key{}", num_items),
                               std::string(100_KiB, 'x'));
        if (ret == cb::engine_errc::temporary_failure ||
            ret == cb::engine_errc::no_memory) {
            wait_for_flusher_to_settle(h);
            wait_for_stat_to_be(
                    h, "ep_checkpoint_memory_pending_destruction", 0);
        }
        num_items++;
    }
    return num_items;
}

uint64_t get_CAS(EngineIface* h, std::string_view key) {
    auto ret = get(h, nullptr, key, Vbid(0));
    checkeq(cb::engine_errc::success, ret.first, "get_CAS: Failed to get key");

    item_info info;
    check_expression(h->get_item_info(*ret.second.get(), info),
                     "get_CAS: Failed to get item info for key");

    return info.cas;
}

cb::EngineErrorItemPair allocate(EngineIface* h,
                                 CookieIface* cookie,
                                 std::string_view key,
                                 size_t nbytes,
                                 int flags,
                                 rel_time_t exptime,
                                 uint8_t datatype,
                                 Vbid vb) {
    bool cookie_created = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        cookie_created = true;
    }

    cb::EngineErrorItemPair ret;
    try {
        ret = {cb::engine_errc::success,
               h->allocateItem(*cookie,
                               DocKeyView(key, DocKeyEncodesCollectionId::No),
                               nbytes,
                               0,
                               flags,
                               exptime,
                               datatype,
                               vb)};
    } catch (const cb::engine_error& error) {
        ret = cb::makeEngineErrorItemPair(
                cb::engine_errc(error.code().value()));
    }

    if (cookie_created) {
        testHarness->destroy_cookie(cookie);
    }

    return ret;
}

cb::EngineErrorItemPair get(EngineIface* h,
                            CookieIface* cookie,
                            std::string_view key,
                            Vbid vb,
                            DocStateFilter documentStateFilter) {
    bool create_cookie = false;
    if (cookie == nullptr) {
        cookie = testHarness->create_cookie(h);
        create_cookie = true;
    }

    auto ret = h->get(*cookie,
                      DocKeyView(key, DocKeyEncodesCollectionId::No),
                      vb,
                      documentStateFilter);

    if (create_cookie) {
        testHarness->destroy_cookie(cookie);
    }
    return ret;
}

bool repeat_till_true(std::function<bool()> functor,
                      uint16_t max_repeat,
                      std::chrono::microseconds sleepTime) {
    bool fSuccess = false;
    do {
        fSuccess = functor();
        if (!fSuccess) {
            std::this_thread::sleep_for(sleepTime);
            max_repeat--;
        }
    } while (!fSuccess && max_repeat > 0);
    return fSuccess;
}

void reset_stats(gsl::not_null<EngineIface*> h) {
    auto* cookie = testHarness->create_cookie(h);
    h->reset_stats(*cookie);
    testHarness->destroy_cookie(cookie);
}

cb::engine_errc get_stats(gsl::not_null<EngineIface*> h,
                          std::string_view key,
                          std::string_view value,
                          const AddStatFn& callback) {
    auto* cookie = testHarness->create_cookie(h);
    auto ret = get_stats_wrapper(h, *cookie, key, value, callback);
    testHarness->destroy_cookie(cookie);
    return ret;
}
