/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mutate_with_meta_context.h"
#include "engine_wrapper.h"
#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <daemon/memcached.h>
#include <logger/logger.h>
#include <mcbp/codec/with_meta_options.h>
#include <mcbp/protocol/datatype.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <nlohmann/json.hpp>
#include <platform/scope_timer.h>
#include <platform/string_hex.h>
#include <utilities/json_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <charconv>

using cb::mcbp::request::MutateWithMetaCommand;

MutateWithMetaCommandContext::MutateWithMetaCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie) {
}

cb::engine_errc MutateWithMetaCommandContext::pre_link_document(
        item_info& info) {
    if ((cas_offsets.empty() && seqno_offsets.empty()) ||
        !cb::mcbp::datatype::is_xattr(info.datatype)) {
        // we have no offsets to insert or there are no xattrs
        return cb::engine_errc::success;
    }

    const auto datatype = static_cast<cb::mcbp::Datatype>(info.datatype);
    LOG_DEBUG_CTX("Pre link document called",
                  {"cas", cb::to_hex(htonll(info.cas))},
                  {"cas_offsets", cas_offsets},
                  {"seqno", cb::to_hex(info.seqno)},
                  {"seqno_offsets", seqno_offsets},
                  {"datatype", to_json(datatype)});

    // Safeguard: The command validation validated that the requested object
    // is not compressed with snappy, so we should never see that here.
    // The value was however passed as view to the underlying engine which
    // allocated a new buffer for the value. Validate that the engine
    // didn't compress the value as that would cause the offsets to be incorrect
    // and we would end up writing the CAS and seqno values at the wrong
    // locations in the document.
    Expects(!cb::mcbp::datatype::is_snappy(info.datatype));
    auto view = cb::char_buffer{static_cast<char*>(info.value[0].iov_base),
                                info.value[0].iov_len};
    // Write CAS value at all specified offsets
    const auto cas_hex = cb::to_hex(htonll(info.cas));
    for (auto cas_offset : cas_offsets) {
        std::copy_n(cas_hex.data(), cas_hex.size(), view.data() + cas_offset);
    }

    // Write seqno value at all specified offsets
    const auto seqno_hex = cb::to_hex(info.seqno);
    for (auto seqno_offset : seqno_offsets) {
        std::copy_n(
                seqno_hex.data(), seqno_hex.size(), view.data() + seqno_offset);
    }

    return cb::engine_errc::success;
}

cb::engine_errc MutateWithMetaCommandContext::step() {
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(
            cookie, cb::tracing::Code::SetWithMeta);

    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::ValidateInput:
            ret = validateInput();
            break;
        case State::StoreItem:
            ret = storeItem();
            break;
        case State::SendResponse:
            if (cookie.getConnection().isSupportsMutationExtras()) {
                mutation_descr.vbucket_uuid =
                        htonll(mutation_descr.vbucket_uuid);
                mutation_descr.seqno = htonll(mutation_descr.seqno);
                cookie.sendResponse(
                        cb::mcbp::Status::Success,
                        {reinterpret_cast<const char*>(&mutation_descr),
                         sizeof(mutation_descr)},
                        {},
                        {},
                        cb::mcbp::Datatype::Raw,
                        cookie.getCas());
            } else {
                cookie.sendResponse(cb::mcbp::Status::Success);
            }
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);
    return ret;
}

std::variant<cb::engine_errc, cb::mcbp::request::MutateWithMetaPayload>
MutateWithMetaCommandContext::parseJson() {
    const auto& request = cookie.getRequest();
    cb::mcbp::request::MutateWithMetaPayload payload;
    try {
        if (request.getExtlen() != sizeof(uint32_t)) {
            cookie.setErrorContext("Invalid extras length");
            return cb::engine_errc::invalid_arguments;
        }

        const auto meta_len = getMetaSize();
        const auto value = cookie.getInflatedInputPayload();
        if (meta_len > value.size()) {
            cookie.setErrorContext("Invalid metadata length");
            return cb::engine_errc::invalid_arguments;
        }

        auto meta = value.substr(value.size() - meta_len);
        payload = nlohmann::json::parse(meta);
    } catch (const std::exception& exception) {
        cookie.setErrorContext(
                fmt::format("Failed to validate input: {}", exception.what()));
        return cb::engine_errc::invalid_arguments;
    }

    return payload;
}

cb::engine_errc MutateWithMetaCommandContext::validateGenerateCasOptions(
        const cb::mcbp::request::MutateWithMetaPayload& payload) {
    if (payload.options.generate_cas == GenerateCas::No) {
        if (!payload.cas.has_value()) {
            cookie.setErrorContext(
                    "CAS must be specified unless options contains "
                    "REGENERATE_CAS");
            return cb::engine_errc::invalid_arguments;
        }
        if (!payload.cas_offsets.empty()) {
            cookie.setErrorContext(
                    "cas_offsets cannot be specified when cas is set");
            return cb::engine_errc::invalid_arguments;
        }
        if (!payload.seqno_offsets.empty()) {
            cookie.setErrorContext(
                    "seqno_offsets cannot be specified when cas is set");
            return cb::engine_errc::invalid_arguments;
        }
        return cb::engine_errc::success;
    }
    if (payload.cas.has_value()) {
        cookie.setErrorContext(
                "CAS cannot be specified when REGENERATE_CAS option is set");
        return cb::engine_errc::invalid_arguments;
    }

    return cb::engine_errc::success;
}

cb::engine_errc MutateWithMetaCommandContext::validateOffsetRequirements(
        std::string_view value, protocol_binary_datatype_t datatype) {
    if (cas_offsets.empty() && seqno_offsets.empty()) {
        return cb::engine_errc::success;
    }

    if (value.empty()) {
        cookie.setErrorContext(
                "Cannot use offset replacements on empty documents");
        return cb::engine_errc::invalid_arguments;
    }

    if (!cb::mcbp::datatype::is_xattr(datatype)) {
        cookie.setErrorContext(
                "Cannot use offset replacements on documents without xattr");
        return cb::engine_errc::invalid_arguments;
    }

    std::string xattr_blob(value.data(), cb::xattr::get_body_offset(value));
    const auto xattr_size = xattr_blob.size();
    // size of the hex representation of 64-bit value with a 0x prefix
    constexpr size_t pattern_size = 18;
    auto fill = cb::to_hex(std::numeric_limits<uint64_t>::max());
    for (auto cas_offset : cas_offsets) {
        if (cas_offset + pattern_size > xattr_size) {
            cookie.setErrorContext(
                    fmt::format("CAS offset [{}, {}] is out of bounds of the "
                                "xattr section [0, {}]",
                                cas_offset,
                                cas_offset + pattern_size,
                                xattr_size));
            return cb::engine_errc::invalid_arguments;
        }
        std::copy(fill.begin(),
                  fill.begin() + fill.size(),
                  xattr_blob.data() + cas_offset);
    }
    for (auto seqno_offset : seqno_offsets) {
        if (seqno_offset + pattern_size > xattr_size) {
            cookie.setErrorContext(
                    fmt::format("Seqno offset [{}, {}] is out of bounds of the "
                                "xattr section [0, {}]",
                                seqno_offset,
                                seqno_offset + pattern_size,
                                xattr_size));
            return cb::engine_errc::invalid_arguments;
        }
        std::copy(fill.begin(),
                  fill.begin() + fill.size(),
                  xattr_blob.data() + seqno_offset);
    }

    // We've patched the blob.. rerun validation to ensure it's still a valid
    // blob if we replace with the pattern.
    if (!cookie.getConnection().getThread().isXattrBlobValid(xattr_blob)) {
        cookie.setErrorContext(
                "Patching seqno and cas would garble the xattr section");
        return cb::engine_errc::invalid_arguments;
    }

    return cb::engine_errc::success;
}

cb::engine_errc MutateWithMetaCommandContext::validateDeleteCommandRequirements(
        std::string_view value, protocol_binary_datatype_t datatype) {
    if (cb::mcbp::datatype::is_xattr(datatype)) {
        if (!value.empty() &&
            cb::xattr::get_body_offset(value) != value.size()) {
            cookie.setErrorContext(
                    "Delete with meta: Document cannot have a body");
            return cb::engine_errc::invalid_arguments;
        }
        cb::char_buffer buffer{const_cast<char*>(value.data()), value.size()};
        cb::xattr::Blob blob(buffer);
        if (blob.has_user_keys()) {
            cookie.setErrorContext(
                    "Delete with meta: user-xattrs is not allowed");
            return cb::engine_errc::invalid_arguments;
        }
    } else if (!value.empty()) {
        cookie.setErrorContext("Delete with meta: Document cannot have a body");
        return cb::engine_errc::invalid_arguments;
    }
    return cb::engine_errc::success;
}

cb::engine_errc MutateWithMetaCommandContext::validateInput() {
    cb::mcbp::request::MutateWithMetaPayload payload;
    {
        auto ret = parseJson();
        if (std::holds_alternative<cb::engine_errc>(ret)) {
            return std::get<cb::engine_errc>(ret);
        }
        payload = std::get<cb::mcbp::request::MutateWithMetaPayload>(ret);
    }

    // flags is stored in network byte order internally
    command = payload.command;
    item_meta.cas = payload.cas.value_or(1);
    item_meta.revSeqno = payload.rev_seqno;
    item_meta.flags = htonl(payload.flags);
    item_meta.exptime = payload.exptime;
    options = payload.options;
    cas_offsets = std::move(payload.cas_offsets);
    seqno_offsets = std::move(payload.seqno_offsets);

    auto ret = validateGenerateCasOptions(payload);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    const auto datatype = static_cast<protocol_binary_datatype_t>(
                                  cookie.getRequest().getDatatype()) &
                          ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    const auto value = getDocumentData();

    ret = validateOffsetRequirements(value, datatype);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    if (command == MutateWithMetaCommand::Delete) {
        ret = validateDeleteCommandRequirements(value, datatype);
        if (ret != cb::engine_errc::success) {
            return ret;
        }
    }

    state = State::StoreItem;
    return cb::engine_errc::success;
}

cb::engine_errc MutateWithMetaCommandContext::storeItem() {
    const auto& request = cookie.getRequest();
    mutation_descr.seqno = 0;
    mutation_descr.vbucket_uuid = std::numeric_limits<uint64_t>::max();
    cb::engine_errc ret;
    uint64_t commandCas = request.getCas();
    const auto datatype = static_cast<protocol_binary_datatype_t>(
                                  cookie.getRequest().getDatatype()) &
                          ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    const auto value = getDocumentData();
    if (command == MutateWithMetaCommand::Delete && value.empty()) {
        ret = bucket_delete_with_meta(cookie,
                                      request.getVBucket(),
                                      cookie.getRequestKey(),
                                      item_meta,
                                      commandCas,
                                      mutation_descr,
                                      options.check_conflicts,
                                      GenerateBySeqno::Yes,
                                      options.generate_cas,
                                      options.delete_source,
                                      options.force_accept);
    } else {
        std::optional<DeleteSource> deleteSource;
        if (command == MutateWithMetaCommand::Delete) {
            deleteSource = options.delete_source;
        }
        auto buffer = cb::const_byte_buffer(
                reinterpret_cast<const uint8_t*>(value.data()), value.size());
        ret = bucket_set_with_meta(cookie,
                                   request.getVBucket(),
                                   cookie.getRequestKey(),
                                   buffer,
                                   item_meta,
                                   deleteSource,
                                   datatype,
                                   commandCas,
                                   mutation_descr,
                                   options.check_conflicts,
                                   command != MutateWithMetaCommand::Add,
                                   GenerateBySeqno::Yes,
                                   options.generate_cas,
                                   options.force_accept);
    }

    if (ret == cb::engine_errc::success) {
        cookie.setCas(commandCas);
        state = State::SendResponse;
    }

    return ret;
}

std::size_t MutateWithMetaCommandContext::getMetaSize() const {
    const auto& extras = cookie.getHeader().getExtdata();
    uint32_t meta_len = 0;
    memcpy(&meta_len, extras.data(), sizeof(meta_len));
    meta_len = ntohl(meta_len);
    return meta_len;
}

std::string_view MutateWithMetaCommandContext::getDocumentData() const {
    auto view = cookie.getInflatedInputPayload();
    return view.substr(0, view.size() - getMetaSize());
}
