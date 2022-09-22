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
#include "remove_context.h"
#include "engine_wrapper.h"

#include <daemon/buckets.h>
#include <memcached/durability_spec.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

cb::engine_errc RemoveCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::GetItem:
            ret = getItem();
            break;
        case State::RebuildXattr:
            ret = rebuildXattr();
            break;
        case State::AllocateDeletedItem:
            ret = allocateDeletedItem();
            break;
        case State::StoreItem:
            ret = storeItem();
            break;
        case State::RemoveItem:
            ret = removeItem();
            break;
        case State::SendResponse:
            ret = sendResponse();
            break;
        case State::Reset:
            ret = reset();
            break;
        case State::Done:
            SLAB_INCR(&connection, delete_hits);
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    if (ret == cb::engine_errc::no_such_key) {
        STATS_INCR(&connection, delete_misses);
    }

    return ret;
}

cb::engine_errc RemoveCommandContext::getItem() {
    auto [status, doc] =
            bucket_get_if(cookie,
                          cookie.getRequestKey(),
                          vbucket,
                          [this](const item_info& info) {
                              existing_cas = info.cas;
                              existing_datatype = info.datatype;
                              return cb::mcbp::datatype::is_xattr(
                                      info.datatype);
                          });

    if (status == cb::engine_errc::success) {
        existing = std::move(doc);
        if (input_cas != 0) {
            if (existing_cas == uint64_t(-1)) {
                // The object in the cache is locked... lets try to use
                // the cas provided by the user to override this
                existing_cas = input_cas;
            } else if (input_cas != existing_cas) {
                return cb::engine_errc::key_already_exists;
            }
        } else if (existing_cas == uint64_t(-1)) {
            return cb::engine_errc::locked;
        }

        if (cb::mcbp::datatype::is_xattr(existing_datatype)) {
            state = State::RebuildXattr;
        } else {
            state = State::RemoveItem;
        }
    }
    return status;
}

cb::engine_errc RemoveCommandContext::allocateDeletedItem() {
    protocol_binary_datatype_t datatype;
    if (xattr.empty()) {
        datatype = PROTOCOL_BINARY_RAW_BYTES;
    } else {
        datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    deleted = bucket_allocate(cookie,
                              cookie.getRequestKey(),
                              xattr.size(),
                              xattr.size(), // Only system xattrs
                              0, // MB-25273: 0 flags when deleting the body
                              0, // Tombstone item, reset expiry time as will
                                 // be removed during purge
                              datatype,
                              vbucket);

    if (input_cas == 0) {
        deleted->setCas(existing_cas);
    } else {
        deleted->setCas(input_cas);
    }

    if (!xattr.empty()) {
        std::copy(
                xattr.begin(), xattr.end(), deleted->getValueBuffer().begin());
    }

    state = State::StoreItem;
    return cb::engine_errc::success;
}

cb::engine_errc RemoveCommandContext::storeItem() {
    uint64_t new_cas;
    auto ret = bucket_store(cookie,
                            deleted.get(),
                            new_cas,
                            StoreSemantics::CAS,
                            {},
                            DocumentState::Deleted,
                            false);

    if (ret == cb::engine_errc::success) {
        item_info info;
        if (!bucket_get_item_info(connection, deleted.get(), &info)) {
            return cb::engine_errc::failed;
        }

        // Response includes vbucket UUID and sequence number
        mutation_descr.vbucket_uuid = info.vbucket_uuid;
        mutation_descr.seqno = info.seqno;
        cookie.setCas(info.cas);

        state = State::SendResponse;
    } else if (ret == cb::engine_errc::key_already_exists && input_cas == 0) {
        // Cas collision and the caller specified the CAS wildcard.. retry
        state = State::Reset;
        ret = cb::engine_errc::success;
    }

    return ret;
}

cb::engine_errc RemoveCommandContext::removeItem() {
    uint64_t new_cas = input_cas;
    const auto& request = cookie.getRequest();
    auto ret = bucket_remove(cookie,
                             cookie.getRequestKey(),
                             new_cas,
                             vbucket,
                             request.getDurabilityRequirements(),
                             mutation_descr);

    if (ret == cb::engine_errc::success) {
        cookie.setCas(new_cas);
        state = State::SendResponse;
    } else if (ret == cb::engine_errc::key_already_exists && input_cas == 0) {
        // Cas collision and the caller specified the CAS wildcard.. retry
        state = State::Reset;
        ret = cb::engine_errc::success;
    }

    return ret;
}

cb::engine_errc RemoveCommandContext::reset() {
    deleted.reset();
    existing.reset();

    xattr = {nullptr, 0};

    state = State::GetItem;
    return cb::engine_errc::success;
}

cb::engine_errc RemoveCommandContext::sendResponse() {
    state = State::Done;

    if (cookie.getRequest().isQuiet()) {
        ++connection.getBucket()
                  .responseCounters[int(cb::mcbp::Status::Success)];
        return cb::engine_errc::success;
    }

    if (connection.isSupportsMutationExtras()) {
        // Response includes vbucket UUID and sequence number
        // Make the byte ordering in the mutation descriptor
        mutation_descr.vbucket_uuid = htonll(mutation_descr.vbucket_uuid);
        mutation_descr.seqno = htonll(mutation_descr.seqno);

        std::string_view extras = {
                reinterpret_cast<const char*>(&mutation_descr),
                sizeof(mutation_descr_t)};

        cookie.sendResponse(cb::mcbp::Status::Success,
                            extras,
                            {},
                            {},
                            cb::mcbp::Datatype::Raw,
                            cookie.getCas());
    } else {
        cookie.sendResponse(cb::mcbp::Status::Success);
    }

    return cb::engine_errc::success;
}

cb::engine_errc RemoveCommandContext::rebuildXattr() {
    if (cb::mcbp::datatype::is_xattr(existing_datatype)) {
        // Create a const blob of the incoming data, which may decompress it
        // Note when writing back the xattrs (if any remain) the snappy bit is
        // never reset, so no need to remember if we did decompress.

        auto view = existing->getValueView();
        const cb::xattr::Blob existingData(
                {const_cast<char*>(view.data()), view.size()},
                cb::mcbp::datatype::is_snappy(existing_datatype));

        // We can't modify the item as when we try to replace the item it
        // may fail due to a race condition (writing back into the existing
        // item). Create a temporary copy of the current value and prune that.
        // Given that we're only going to (potentially) remove data in the xattr
        // blob, it will only _shrink_ in size so we  don't need to pass on the
        // allocator to the blob
        xattr_buffer.reset(new char[existingData.size()]);
        std::copy_n(
                existingData.data(), existingData.size(), xattr_buffer.get());

        // Now prune the copy
        cb::xattr::Blob blob({xattr_buffer.get(), existingData.size()},
                             false /* data is not compressed*/);
        blob.prune_user_keys();
        xattr = blob.finalize();
        if (!xattr.empty() && (xattr.data() != xattr_buffer.get())) {
            throw std::logic_error(
                    "RemoveCommandContext::rebuildXattr: Internal error. No "
                    "reallocations should happend when pruning user "
                    "attributes");
        }
    }

    if (!xattr.empty()) {
        state = State::AllocateDeletedItem;
    } else {
        // All xattrs should be nuked
        state = State::RemoveItem;
    }

    return cb::engine_errc::success;
}
