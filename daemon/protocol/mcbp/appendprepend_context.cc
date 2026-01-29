/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "appendprepend_context.h"
#include "engine_wrapper.h"
#include "item_dissector.h"
#include <daemon/cookie.h>
#include <daemon/thread_stats.h>
#include <folly/io/IOBuf.h>
#include <memcached/durability_spec.h>
#include <xattr/blob.h>

AppendPrependCommandContext::AppendPrependCommandContext(
        Cookie& cookie, const cb::mcbp::Request& req)
    : SteppableCommandContext(cookie),
      mode((req.getClientOpcode() == cb::mcbp::ClientOpcode::Append ||
            req.getClientOpcode() == cb::mcbp::ClientOpcode::Appendq)
                   ? Mode::Append
                   : Mode::Prepend),
      vbucket(req.getVBucket()),
      cas(req.getCas()),
      state(State::GetItem) {
}

cb::engine_errc AppendPrependCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::GetItem:
            ret = getItem();
            break;
        case State::AllocateNewItem:
            ret = allocateNewItem();
            break;
        case State::StoreItem:
            ret = storeItem();
            break;
        case State::Reset:
            ret = reset();
            break;
        case State::Done:
	    SLAB_INCR(&connection, cmd_set);
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    if (ret == cb::engine_errc::no_such_key) {
        // for some reason the return code for no key is not stored so we need
        // to remap that error code..
        ret = cb::engine_errc::not_stored;
    }

    if (ret != cb::engine_errc::would_block) {
        SLAB_INCR(&connection, cmd_set);
    }

    return ret;
}

cb::engine_errc AppendPrependCommandContext::getItem() {
    auto [status, document] =
            bucket_get(cookie, cookie.getRequestKey(), vbucket);
    if (status == cb::engine_errc::success) {
        old_item_cas = document->getCas();
        if (cas != 0) {
            if (old_item_cas == uint64_t(-1)) {
                // The object in the cache is locked... lets try to use
                // the cas provided by the user to override this
                old_item_cas = cas;
            } else if (cas != old_item_cas) {
                return cb::engine_errc::key_already_exists;
            }
        } else if (old_item_cas == uint64_t(-1)) {
            return cb::engine_errc::locked;
        }
        old_item = std::make_unique<ItemDissector>(
                cookie, std::move(document), true);

        // Move on to the next state
        state = State::AllocateNewItem;
    }

    return status;
}

cb::engine_errc AppendPrependCommandContext::allocateNewItem() {
    // If we're operating on a document containing xattr's we need to
    // tell the underlying engine about how much of the data which
    // should be accounted for in the privileged segment.
    size_t priv_bytes = 0;
    const auto xattrs = old_item->getExtendedAttributes();
    auto datatype = PROTOCOL_BINARY_RAW_BYTES;

    if (!xattrs.empty()) {
        cb::xattr::Blob blob({const_cast<char*>(xattrs.data()), xattrs.size()},
                             false);
        priv_bytes = blob.get_system_size();
        datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }
    const auto& document = old_item->getItem();
    auto old_value = old_item->getValue();

    // If the client sent a compressed value we should use the one
    // we inflated
    const auto value = cookie.getInflatedInputPayload();
    newitem = bucket_allocate(cookie,
                              cookie.getRequestKey(),
                              xattrs.size() + old_value.size() + value.size(),
                              priv_bytes,
                              document.getFlags(),
                              rel_time_t(document.getExptime()),
                              datatype,
                              vbucket);
    auto body = newitem->getValueBuffer();

    // copy the data over...
    if (mode == Mode::Append) {
        fmt::format_to(body.begin(), "{}{}{}", xattrs, old_value, value);
    } else {
        fmt::format_to(body.begin(), "{}{}{}", xattrs, value, old_value);
    }

    // If the resulting document's data is valid JSON, set the datatype flag
    // to reflect this.
    const auto buf = std::string_view{body.data() + xattrs.size(),
                                      body.size() - xattrs.size()};
    // Update the documents datatype and CAS values
    setDatatypeJSONFromValue(buf, datatype);
    newitem->setDataType(datatype);
    newitem->setCas(old_item_cas);
    state = State::StoreItem;
    return cb::engine_errc::success;
}

cb::engine_errc AppendPrependCommandContext::storeItem() {
    uint64_t ncas = cas;
    auto ret = bucket_store(cookie,
                            *newitem,
                            ncas,
                            StoreSemantics::CAS,
                            cookie.getRequest().getDurabilityRequirements(),
                            DocumentState::Alive,
                            false);

    if (ret == cb::engine_errc::success) {
        cookie.setCas(ncas);
        if (connection.isSupportsMutationExtras()) {
            item_info newItemInfo;
            if (!bucket_get_item_info(connection, *newitem, newItemInfo)) {
                return cb::engine_errc::failed;
            }
            mutation_descr_t extras = {};
            extras.vbucket_uuid = htonll(newItemInfo.vbucket_uuid);
            extras.seqno = htonll(newItemInfo.seqno);
            cookie.sendResponse(
                    cb::mcbp::Status::Success,
                    {reinterpret_cast<const char*>(&extras), sizeof(extras)},
                    {},
                    {},
                    cb::mcbp::Datatype::Raw,
                    ncas);
        } else {
            cookie.sendResponse(cb::mcbp::Status::Success);
        }
        state = State::Done;
    } else if (ret == cb::engine_errc::key_already_exists && cas == 0) {
        state = State::Reset;
        // We need to return cb::engine_errc::success in order to continue
        // processing
        ret = cb::engine_errc::success;
    }

    return ret;
}

cb::engine_errc AppendPrependCommandContext::reset() {
    old_item.reset();
    newitem.reset();
    state = State::GetItem;
    return cb::engine_errc::success;
}
