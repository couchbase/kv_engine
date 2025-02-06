/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "subdocument_context.h"

#include "front_end_thread.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "settings.h"
#include "subdocument_parser.h"
#include "subdocument_validators.h"
#include <gsl/gsl-lite.hpp>
#include <logger/logger.h>
#include <platform/crc32c.h>
#include <platform/string_hex.h>
#include <subdoc/util.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/string_utilities.h>
#include <xattr/blob.h>
#include <xattr/key_validator.h>
#include <iomanip>
#include <random>
#include <sstream>
#include <utility>

using namespace std::string_literals;
using namespace std::string_view_literals;

SubdocExecutionContext::OperationSpec::OperationSpec(
        SubdocCmdTraits traits_,
        cb::mcbp::subdoc::PathFlag flags_,
        std::string path_,
        std::string value_)
    : traits(traits_),
      flags(flags_),
      path(std::move(path_)),
      value(std::move(value_)),
      status(cb::mcbp::Status::Einternal) {
    if (hasMkdirP(flags)) {
        traits.subdocCommand = Subdoc::Command(traits.subdocCommand |
                                               Subdoc::Command::FLAG_MKDIR_P);
    }
}

SubdocExecutionContext::OperationSpec::OperationSpec(OperationSpec&& other)
    : traits(other.traits),
      flags(other.flags),
      path(other.path),
      value(other.value) {
}

uint64_t SubdocExecutionContext::getOperationValueBytesTotal() const {
    uint64_t result = 0;
    for (auto& ops : operations) {
        for (auto& op : ops) {
            result += op.value.size();
        }
    }
    return result;
}

/*
 * Definitions
 */
void SubdocExecutionContext::create_single_path_context(
        cb::mcbp::subdoc::DocFlag doc_flags) {
    const auto& request = cookie.getRequest();
    const auto extras = request.getExtdata();
    cb::mcbp::request::SubdocPayloadParser parser(extras);
    const auto pathlen = parser.getPathlen();
    auto flags = parser.getSubdocFlags();
    const auto valbuf = request.getValue();
    std::string_view value{reinterpret_cast<const char*>(valbuf.data()),
                           valbuf.size()};
    // Path is the first thing in the value; remainder is the operation
    // value.
    auto path = value.substr(0, pathlen);

    const bool xattr = hasXattrPath(flags);
    const SubdocExecutionContext::Phase phase =
            xattr ? SubdocExecutionContext::Phase::XATTR
                  : SubdocExecutionContext::Phase::Body;
    auto& ops = getOperations(phase);

    if (hasExpandedMacros(flags)) {
        do_macro_expansion = true;
    }

    // If Mkdoc or Add is specified, this implies MKDIR_P, ensure that it's set
    // here
    if (impliesMkdir_p(doc_flags)) {
        flags = flags | cb::mcbp::subdoc::PathFlag::Mkdir_p;
    }

    // Decode as single path; add a single operation to the context.
    if (traits.request_has_value) {
        // Adjust value to move past the path.
        value.remove_prefix(pathlen);

        ops.emplace_back(SubdocExecutionContext::OperationSpec{
                traits,
                flags,
                {path.data(), path.size()},
                {value.data(), value.size()}});
    } else {
        ops.emplace_back(SubdocExecutionContext::OperationSpec{
                traits, flags, {path.data(), path.size()}});
    }

    if (impliesMkdir_p(doc_flags) && createState == DocumentState::Alive) {
        // For requests to create the key in the Alive state, set the JSON
        // of the to-be-created document root.
        jroot_type = Subdoc::Util::get_root_type(
                traits.subdocCommand, path.data(), path.size());
    }
}

void SubdocExecutionContext::create_multi_path_context(
        cb::mcbp::subdoc::DocFlag doc_flags) {
    // Decode each of lookup specs from the value into our command context.
    const auto& request = cookie.getRequest();
    const auto valbuf = request.getValue();
    std::string_view value{reinterpret_cast<const char*>(valbuf.data()),
                           valbuf.size()};

    size_t offset = 0;
    while (offset < value.size()) {
        cb::mcbp::ClientOpcode binprot_cmd;
        cb::mcbp::subdoc::PathFlag flags;
        size_t headerlen;
        std::string_view path;
        std::string_view spec_value;
        if (traits.is_mutator) {
            auto* spec = reinterpret_cast<
                    const protocol_binary_subdoc_multi_mutation_spec*>(
                    value.data() + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = cb::mcbp::ClientOpcode(spec->opcode);
            flags = cb::mcbp::subdoc::PathFlag(spec->flags);
            path = {value.data() + offset + headerlen, htons(spec->pathlen)};
            spec_value = {value.data() + offset + headerlen + path.size(),
                          htonl(spec->valuelen)};

        } else {
            auto* spec = reinterpret_cast<
                    const protocol_binary_subdoc_multi_lookup_spec*>(
                    value.data() + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = cb::mcbp::ClientOpcode(spec->opcode);
            flags = cb::mcbp::subdoc::PathFlag(spec->flags);
            path = {value.data() + offset + headerlen, htons(spec->pathlen)};
            spec_value = {nullptr, 0};
        }

        auto cmdTraits = get_subdoc_cmd_traits(binprot_cmd);
        if (cmdTraits.scope == CommandScope::SubJSON &&
            impliesMkdir_p(doc_flags) && createState == DocumentState::Alive &&
            jroot_type == JSONSL_T_ROOT) {
            // For requests to create the key in the Alive state, set the JSON
            // of the to-be-created document root.
            jroot_type = Subdoc::Util::get_root_type(
                    cmdTraits.subdocCommand, path.data(), path.size());
        }

        if (hasExpandedMacros(flags)) {
            do_macro_expansion = true;
        }

        const bool xattr = hasXattrPath(flags);
        const SubdocExecutionContext::Phase phase =
                xattr ? SubdocExecutionContext::Phase::XATTR
                      : SubdocExecutionContext::Phase::Body;

        auto& ops = getOperations(phase);

        // Mkdoc and Add imply MKDIR_P, ensure that MKDIR_P is set
        if (impliesMkdir_p(doc_flags)) {
            flags = flags | cb::mcbp::subdoc::PathFlag::Mkdir_p;
        }
        if (cmdTraits.mcbpCommand == cb::mcbp::ClientOpcode::Delete) {
            do_delete_doc = true;
        }
        ops.emplace_back(SubdocExecutionContext::OperationSpec{
                cmdTraits,
                flags,
                {path.data(), path.size()},
                {spec_value.data(), spec_value.size()}});
        offset += headerlen + path.size() + spec_value.size();
    }
}

SubdocExecutionContext::SubdocExecutionContext(
        Cookie& cookie_,
        const SubdocCmdTraits& traits_,
        Vbid vbucket_,
        cb::mcbp::subdoc::DocFlag doc_flags)
    : cookie(cookie_),
      connection(cookie_.getConnection()),
      traits(traits_),
      vbucket(vbucket_),
      do_allow_deleted_docs(cb::mcbp::subdoc::hasAccessDeleted(doc_flags)),
      do_read_replica(cb::mcbp::subdoc::hasReplicaRead(doc_flags)),
      createState(cb::mcbp::subdoc::hasCreateAsDeleted(doc_flags)
                          ? DocumentState::Deleted
                          : DocumentState::Alive),
      mutationSemantics(cb::mcbp::subdoc::hasAdd(doc_flags)
                                ? MutationSemantics::Add
                        : cb::mcbp::subdoc::hasMkdoc(doc_flags)
                                ? MutationSemantics::Set
                                : MutationSemantics::Replace),
      reviveDocument(cb::mcbp::subdoc::hasReviveDocument(doc_flags)) {
    switch (traits.path) {
    case SubdocPath::SINGLE:
        create_single_path_context(doc_flags);
        break;

    case SubdocPath::MULTI:
        create_multi_path_context(doc_flags);
        break;
    }
}

template <typename T>
std::string SubdocExecutionContext::macroToString(
        cb::xattr::macros::macro macro, T macroValue) {
    if (sizeof(T) != macro.expandedSize) {
        throw std::logic_error("macroToString: Invalid size specified for " +
                               std::string(macro.name));
    }
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    ss << "\"0x" << std::setw(sizeof(T) * 2) << macroValue << "\"";
    return ss.str();
}

cb::engine_errc SubdocExecutionContext::pre_link_document(item_info& info) {
    if (do_macro_expansion && cb::mcbp::datatype::is_xattr(info.datatype)) {
        cb::char_buffer blob_buffer{static_cast<char*>(info.value[0].iov_base),
                                    info.value[0].iov_len};
        // Subdoc operates on inflated versions of the document
        Expects(!cb::mcbp::datatype::is_snappy(info.datatype));
        cb::xattr::Blob xattr_blob(blob_buffer, false);

        // The same xattr key may be used multiple times in the
        // spec. Loop over the xattr spec and pick out the keys we
        // want to operate on and insert them into a set so that
        // we only try to replace the macro in the blob _once_
        std::unordered_set<std::string> keys;

        for (const auto& op :
             getOperations(SubdocExecutionContext::Phase::XATTR)) {
            if (hasExpandedMacros(op.flags)) {
                size_t xattr_keylen;
                is_valid_xattr_key({op.path.data(), op.path.size()},
                                   xattr_keylen);
                std::string_view key{op.path.data(), xattr_keylen};
                keys.insert(std::string{op.path.data(), xattr_keylen});
            }
        }

        for (const auto& key : keys) {
            auto view = xattr_blob.get(key);
            if (view.empty()) {
                // The segment is no longer there (we may have had another
                // subdoc command which rewrote the segment where we injected
                // the macro.
                continue;
            }
            auto value = cb::char_buffer{const_cast<char*>(view.data()),
                                         view.size()};

            // Replace the CAS
            if (containsMacro(cb::xattr::macros::CAS)) {
                substituteMacro(
                        cb::xattr::macros::CAS,
                        macroToString(cb::xattr::macros::CAS, htonll(info.cas)),
                        value);
            }

            // Replace the Seqno
            if (containsMacro(cb::xattr::macros::SEQNO)) {
                substituteMacro(
                        cb::xattr::macros::SEQNO,
                        macroToString(cb::xattr::macros::SEQNO, info.seqno),
                        value);
            }

            // Replace the Value CRC32C
            if (containsMacro(cb::xattr::macros::VALUE_CRC32C)) {
                substituteMacro(cb::xattr::macros::VALUE_CRC32C,
                                macroToString(cb::xattr::macros::VALUE_CRC32C,
                                              computeValueCRC32C()),
                                value);
            }
        }
    }

    return cb::engine_errc::success;
}

bool SubdocExecutionContext::containsMacro(cb::xattr::macros::macro macro) {
    return std::any_of(std::begin(paddedMacros),
                       std::end(paddedMacros),
                       [&macro](const MacroPair& m) {
                           return m.first == macro.name;
                       });
}

void SubdocExecutionContext::substituteMacro(cb::xattr::macros::macro macroName,
                                             const std::string& macroValue,
                                             cb::char_buffer& value) {
    // Do an in-place substitution of the real macro value where we
    // wrote the padded macro string.
    char* root = value.begin();
    char* end = value.end();
    auto& macro = std::find_if(std::begin(paddedMacros),
                               std::end(paddedMacros),
                               [macroName](const MacroPair& m) {
                                   return m.first == macroName.name;
                               })
                          ->second;
    auto* needle = macro.data();
    auto* needle_end = macro.data() + macro.length();

    // This replaces ALL instances of the padded string
    while ((root = std::search(root, end, needle, needle_end)) != end) {
        std::copy_n(macroValue.data(), macroValue.length(), root);
        root += macroValue.length();
    }
}

std::string_view SubdocExecutionContext::expand_virtual_macro(
        std::string_view macro) {
    if (macro.find(R"("${$document)") == 0) {
        return expand_virtual_document_macro(macro);
    }

    return {};
}

std::string_view SubdocExecutionContext::expand_virtual_document_macro(
        std::string_view macro) {
    if (macro == R"("${$document}")") {
        // the entire document is requested!
        return get_document_vattr();
    }
    // Skip the "${$document prefix
    macro = {macro.data() + 12, macro.size() - 12};
    if (macro == R"(.CAS}")") {
        expandedVirtualMacrosBackingStore.emplace_back(
                "\"" + cb::to_hex(input_item_info.cas) + "\"");
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.vbucket_uuid}")") {
        expandedVirtualMacrosBackingStore.emplace_back(
                "\"" + cb::to_hex(input_item_info.vbucket_uuid) + "\"");
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.seqno}")") {
        expandedVirtualMacrosBackingStore.emplace_back(
                "\"" + cb::to_hex(input_item_info.seqno) + "\"");
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.revid}")") {
        expandedVirtualMacrosBackingStore.emplace_back(
            "\"" + std::to_string(input_item_info.revid) + "\"");
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.exptime}")") {
        expandedVirtualMacrosBackingStore.emplace_back(
                std::to_string(uint64_t(input_item_info.exptime)));
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.flags}")") {
        // The flags are kept internally in network byte order...
        expandedVirtualMacrosBackingStore.emplace_back(
                std::to_string(uint32_t(ntohl(input_item_info.flags))));
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.value_bytes}")") {
        // Calculate value_bytes (excluding XATTR). Note we use
        // in_datatype / in_doc here as they have already been
        // decompressed for us (see get_document_for_searching).
        size_t value_bytes = in_doc.view.size();
        if (cb::mcbp::datatype::is_xattr(in_datatype)) {
            // strip off xattr
            auto bodyoffset = cb::xattr::get_body_offset(in_doc.view);
            value_bytes -= bodyoffset;
        }
        expandedVirtualMacrosBackingStore.emplace_back(
                std::to_string(value_bytes));
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.datatype}")") { // Calculate datatype[]. Note we use the
        // original datatype
        // (input_item_info.datatype), so if the document was
        // originally compressed we'll report it here.
        nlohmann::json arr;
        auto datatypes = split_string(
                cb::mcbp::datatype::to_string(input_item_info.datatype), ",");
        for (const auto& d : datatypes) {
            arr.push_back(d);
        }
        expandedVirtualMacrosBackingStore.emplace_back(arr.dump());
        return expandedVirtualMacrosBackingStore.back();
    }
    if (macro == R"(.deleted}")") {
        expandedVirtualMacrosBackingStore.emplace_back(
                input_item_info.document_state == DocumentState::Deleted
                        ? "true"
                        : "false");
        return expandedVirtualMacrosBackingStore.back();
    }
    if (input_item_info.cas_is_hlc) {
        if (macro == R"(.last_modified}")") {
            std::chrono::nanoseconds ns(input_item_info.cas);
            expandedVirtualMacrosBackingStore.emplace_back(std::to_string(
                    std::chrono::duration_cast<std::chrono::seconds>(ns)
                            .count()));
            return expandedVirtualMacrosBackingStore.back();
        }
    }

    return {};
}

std::string_view SubdocExecutionContext::get_padded_macro(
        std::string_view macro) {
    auto iter = std::find_if(
            std::begin(paddedMacros),
            std::end(paddedMacros),
            [macro](const MacroPair& a) { return a.first == macro; });
    if (iter == paddedMacros.end()) {
        return {};
    }

    return iter->second;
}

void SubdocExecutionContext::generate_macro_padding(
        std::string_view payload, cb::xattr::macros::macro macro) {
    if (!do_macro_expansion) {
        // macro expansion is not needed
        return;
    }

    bool unique = false;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    while (!unique) {
        unique = true;
        uint64_t ii = dis(gen);

        std::string candidate;
        switch (macro.expandedSize) {
        case 8:
            candidate = "\"" + cb::to_hex(ii) + "\"";
            break;
        case 4:
            candidate =
                    "\"" + cb::to_hex(gsl::narrow_cast<uint32_t>(ii)) + "\"";
            break;
        default:
            throw std::logic_error(
                    "generate_macro_padding: invalid macro expandedSize: " +
                    std::to_string(macro.expandedSize));
        }

        for (auto& op : getOperations(Phase::XATTR)) {
            if (op.value.find(candidate, 0) != std::string_view::npos) {
                unique = false;
                break;
            }
        }

        if (unique) {
            if (payload.find(candidate, 0) != std::string_view::npos) {
                unique = false;
            } else {
                paddedMacros.emplace_back(macro.name, candidate);
            }
        }
    }
}

void SubdocExecutionContext::rewrite_in_document(std::string_view xattr,
                                                 std::string_view value) {
    if (xattr.empty()) {
        in_doc.reset(std::string(value));
        in_datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
        no_sys_xattrs = true;
    } else {
        in_doc.reset(fmt::format("{}{}", xattr, value));
        in_datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }
}

std::string_view SubdocExecutionContext::get_document_vattr() {
    if (document_vattr.empty()) {
        // @todo we can optimize this by building the json in a more efficient
        //       way, but for now just do it by using nlohmann json...
        nlohmann::json doc;

        doc["CAS"] = cb::to_hex(input_item_info.cas);
        doc["vbucket_uuid"] = cb::to_hex(input_item_info.vbucket_uuid);
        doc["seqno"] = cb::to_hex(input_item_info.seqno);
        doc["revid"] = std::to_string(input_item_info.revid);
        doc["exptime"] = input_item_info.exptime;

        // The flags are kept internally in network byte order...
        doc["flags"] = ntohl(input_item_info.flags);

        // Calculate value_bytes (excluding XATTR). Note we use
        // in_datatype / in_doc here as they have already been
        // decompressed for us (see get_document_for_searching).
        size_t value_bytes = in_doc.view.size();
        if (cb::mcbp::datatype::is_xattr(in_datatype)) {
            // strip off xattr
            auto bodyoffset = cb::xattr::get_body_offset(in_doc.view);
            value_bytes -= bodyoffset;
        }
        doc["value_bytes"] = value_bytes;

        // Calculate datatype[]. Note we use the original datatype
        // (input_item_info.datatype), so if the document was
        // originally compressed we'll report it here.
        nlohmann::json arr;
        auto datatypes = split_string(
                cb::mcbp::datatype::to_string(input_item_info.datatype), ",");
        for (const auto& d : datatypes) {
            arr.push_back(d);
        }
        doc["datatype"] = arr;

        doc["deleted"] =
                input_item_info.document_state == DocumentState::Deleted;

        if (input_item_info.cas_is_hlc) {
            std::chrono::nanoseconds ns(input_item_info.cas);
            doc["last_modified"] = std::to_string(
                    std::chrono::duration_cast<std::chrono::seconds>(ns)
                            .count());
        }

        // Given that the crc32c is of the _value_ (and not the xattrs)
        // we can still include that even when we modify the document
        // as virtual macros must be executed _BEFORE_ we start updating
        // the payload
        doc["value_crc32c"] = cb::to_hex(computeValueCRC32C());

        if (traits.is_mutator) {
            // When used from within a macro expansion we don't want
            // to inject an extra level of "$document":
            document_vattr = doc.dump();
        } else {
            nlohmann::json root;
            root["$document"] = doc;
            document_vattr = root.dump();
        }
    }

    return document_vattr;
}

std::string_view SubdocExecutionContext::get_vbucket_vattr() {
    if (vbucket_vattr.empty()) {
        auto hlc = connection.getBucketEngine().getVBucketHlcNow(vbucket);
        // hlc valid if vbucket exists
        if (hlc) {
            using namespace nlohmann;
            std::string mode = (hlc->mode == cb::HlcTime::Mode::Real)
                                       ? "real"s
                                       : "logical"s;
            json root = {{"$vbucket",
                          {{"HLC",
                            {{"now", std::to_string(hlc->now.count())},
                             {"mode", mode}}}}}};
            vbucket_vattr = root.dump();
        }
    }

    return vbucket_vattr;
}

std::string_view SubdocExecutionContext::get_xtoc_vattr() {
    if (!cb::mcbp::datatype::is_xattr(in_datatype)) {
        xtoc_vattr = R"({"$XTOC":[]})";
        return xtoc_vattr;
    }
    if (xtoc_vattr.empty()) {
        const auto bodyoffset = cb::xattr::get_body_offset(in_doc.view);
        cb::char_buffer blob_buffer{const_cast<char*>(in_doc.view.data()),
                                    bodyoffset};
        cb::xattr::Blob xattr_blob(blob_buffer,
                                   cb::mcbp::datatype::is_snappy(in_datatype));

        nlohmann::json arr = nlohmann::json::array();
        for (const auto [key, value] : xattr_blob) {
            (void)value;
            bool isSystemXattr = cb::xattr::is_system_xattr(key);

            if (xtocSemantics == XtocSemantics::All ||
                (!isSystemXattr && (xtocSemantics == XtocSemantics::User))) {
                arr.push_back(key);
            }
        }

        nlohmann::json doc;
        doc["$XTOC"] = arr;
        xtoc_vattr = doc.dump();
    }
    return xtoc_vattr;
}

cb::engine_errc SubdocExecutionContext::get_document_for_searching(
        uint64_t client_cas) {
    item_info& info = getInputItemInfo();
    auto& c = connection;

    if (!bucket_get_item_info(connection, *fetchedItem, info)) {
        LOG_WARNING_CTX("Failed to get item info", {"conn_id", c.getId()});
        return cb::engine_errc::failed;
    }
    if (info.cas == LOCKED_CAS) {
        // Check that item is not locked:
        if (client_cas == 0 || client_cas == LOCKED_CAS) {
            return cb::engine_errc::locked_tmpfail;
        }
        // If the user *did* supply the CAS, we will validate it later on
        // when the mutation is actually applied. In any event, we don't
        // run the following branch on locked documents.
    } else if ((client_cas != 0) && client_cas != info.cas) {
        // Check CAS matches (if specified by the user).
        return cb::engine_errc::key_already_exists;
    }

    in_flags = info.flags;
    in_cas = client_cas ? client_cas : info.cas;
    in_doc = MemoryBackedBuffer{{static_cast<char*>(info.value[0].iov_base),
                                 info.value[0].iov_len}};
    in_datatype = info.datatype;
    in_document_state = info.document_state;

    if (cb::mcbp::datatype::is_snappy(info.datatype)) {
        // Need to expand before attempting to extract from it.
        try {
            inflated_doc = cookie.inflateSnappy(in_doc.view);
        } catch (const std::runtime_error&) {
            LOG_ERROR_CTX(
                    "Failed to inflate body, key may have an incorrect "
                    "datatype",
                    {"conn_id", cookie.getConnectionId()},
                    {"key",
                     cb::UserDataView(
                             cookie.getRequestKey().toPrintableString())},
                    {"datatype", info.datatype});
            return cb::engine_errc::failed;
        } catch (const std::bad_alloc&) {
            return cb::engine_errc::no_memory;
        }

        // Update the document to point to the uncompressed version.
        auto range = inflated_doc->coalesce();
        std::string_view view{reinterpret_cast<const char*>(range.data()),
                              range.size()};
        in_doc = MemoryBackedBuffer{view};
        in_datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    }

    return cb::engine_errc::success;
}

uint32_t SubdocExecutionContext::computeValueCRC32C() {
    std::string_view value;
    if (cb::mcbp::datatype::is_xattr(in_datatype)) {
        // Note: in the XAttr naming, body/value excludes XAttrs
        value = cb::xattr::get_body(in_doc.view);
    } else {
        value = in_doc.view;
    }
    return crc32c(value);
}

cb::mcbp::Status SubdocExecutionContext::operate_one_path(
        SubdocExecutionContext::OperationSpec& spec, std::string_view view) {
    // Prepare the specified sub-document command.
    auto& op = connection.getThread().subdoc_op;
    op.clear();
    op.set_result_buf(&spec.result);
    op.set_code(spec.traits.subdocCommand);
    op.set_doc(view.data(), view.size());

    if (hasExpandedMacros(spec.flags)) {
        auto padded_macro = get_padded_macro(spec.value);
        if (padded_macro.empty()) {
            padded_macro = expand_virtual_macro(spec.value);
            if (padded_macro.empty()) {
                return cb::mcbp::Status::SubdocXattrUnknownVattrMacro;
            }
        }
        op.set_value(padded_macro.data(), padded_macro.size());
    } else if (spec.traits.is_mutator && hasBinaryValue(spec.flags)) {
        binaryEncodedStorage.emplace_back(base64_encode_value(spec.value));
        op.set_value(binaryEncodedStorage.back());
    } else {
        op.set_value(spec.value.data(), spec.value.size());
    }

    if (getCurrentPhase() == SubdocExecutionContext::Phase::XATTR &&
        cb::xattr::is_vattr(spec.path)) {
        // path potentially contains elements with in the VATTR, e.g.
        // $document.cas. Need to extract the actual VATTR name prefix.
        auto vattr_key = spec.path;
        auto key_end = spec.path.find_first_of(".[");
        if (key_end != std::string::npos) {
            vattr_key.resize(key_end);
        }

        // Check which VATTR is being accessed:
        std::string_view doc;
        if (vattr_key == cb::xattr::vattrs::DOCUMENT) {
            // This is a call to the "$document" VATTR, so replace the document
            // with the document virtual one..
            doc = get_document_vattr();
        } else if (vattr_key == cb::xattr::vattrs::XTOC) {
            doc = get_xtoc_vattr();
        } else if (vattr_key == cb::xattr::vattrs::VBUCKET) {
            // replace the document with the vbucket virtual one..
            doc = get_vbucket_vattr();
            if (doc.empty()) {
                // operation's vbucket does not exist
                return cb::mcbp::Status::NotMyVbucket;
            }
        } else {
            return cb::mcbp::Status::SubdocXattrUnknownVattr;
        }
        op.set_doc(doc.data(), doc.size());
    }

    // ... and execute it.
    const auto subdoc_res = op.op_exec(spec.path.data(), spec.path.size());

    switch (subdoc_res) {
    case Subdoc::Error::SUCCESS:
        if (spec.traits.subdocCommand == Subdoc::Command::GET &&
            hasBinaryValue(spec.flags)) {
            try {
                auto value =
                        base64_decode_value(spec.result.matchloc().to_view());
                binaryEncodedStorage.emplace_back(std::move(value));
                spec.result.set_matchloc({binaryEncodedStorage.back().data(),
                                          binaryEncodedStorage.back().size()});
            } catch (const std::exception&) {
                return cb::mcbp::Status::SubdocFieldNotBinaryValue;
            }
        }

        return cb::mcbp::Status::Success;

    case Subdoc::Error::PATH_ENOENT:
        return cb::mcbp::Status::SubdocPathEnoent;

    case Subdoc::Error::PATH_MISMATCH:
        return cb::mcbp::Status::SubdocPathMismatch;

    case Subdoc::Error::DOC_ETOODEEP:
        return cb::mcbp::Status::SubdocDocE2deep;

    case Subdoc::Error::PATH_EINVAL:
        return cb::mcbp::Status::SubdocPathEinval;

    case Subdoc::Error::DOC_NOTJSON:
        return cb::mcbp::Status::SubdocDocNotJson;

    case Subdoc::Error::DOC_EEXISTS:
        return cb::mcbp::Status::SubdocPathEexists;

    case Subdoc::Error::PATH_E2BIG:
        return cb::mcbp::Status::SubdocPathE2big;

    case Subdoc::Error::NUM_E2BIG:
        return cb::mcbp::Status::SubdocNumErange;

    case Subdoc::Error::DELTA_EINVAL:
        return cb::mcbp::Status::SubdocDeltaEinval;

    case Subdoc::Error::VALUE_CANTINSERT:
        return cb::mcbp::Status::SubdocValueCantinsert;

    case Subdoc::Error::DELTA_OVERFLOW:
        return cb::mcbp::Status::SubdocValueCantinsert;

    case Subdoc::Error::VALUE_ETOODEEP:
        return cb::mcbp::Status::SubdocValueEtoodeep;

    default:
        // TODO: handle remaining errors.
        LOG_WARNING_CTX(
                "Unexpected response from subdoc",
                {"conn_id", cookie.getConnectionId()},
                {"ref", cookie.getEventId()},
                {"description", subdoc_res.description()},
                {"code", fmt::format("0x{:x}", int(subdoc_res.code()))});
        return cb::mcbp::Status::Einternal;
    }
}

cb::mcbp::Status SubdocExecutionContext::operate_wholedoc(
        SubdocExecutionContext::OperationSpec& spec, std::string_view& doc) {
    switch (spec.traits.mcbpCommand) {
    case cb::mcbp::ClientOpcode::Get:
        if (doc.empty()) {
            // Size of zero indicates the document body ("path") doesn't exist.
            return cb::mcbp::Status::SubdocPathEnoent;
        }
        spec.result.set_matchloc({doc.data(), doc.size()});
        return cb::mcbp::Status::Success;

    case cb::mcbp::ClientOpcode::Set:
        spec.result.push_newdoc({spec.value.data(), spec.value.size()});
        return cb::mcbp::Status::Success;

    case cb::mcbp::ClientOpcode::Delete:
        in_datatype &= ~BODY_ONLY_DATATYPE_MASK;
        spec.result.push_newdoc({nullptr, 0});
        return cb::mcbp::Status::Success;

    default:
        return cb::mcbp::Status::Einval;
    }
}

cb::mcbp::Status SubdocExecutionContext::operate_attributes_and_body(
        SubdocExecutionContext::OperationSpec& spec,
        MemoryBackedBuffer* xattr,
        MemoryBackedBuffer& body) {
    if (spec.traits.mcbpCommand !=
        cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr) {
        return cb::mcbp::Status::NotSupported;
    }

    if (xattr == nullptr) {
        throw std::invalid_argument(
                "operate_attributes_and_body: can't be called with "
                "xattr being nullptr");
    }

    // Currently we only support SubdocReplaceBodyWithXattr through this
    // API, and we can implement that by using a SUBDOC-GET to get the location
    // of the data; and then reset the body with the content of the operation.
    auto st = operate_one_path(spec, xattr->view);
    if (st == cb::mcbp::Status::Success) {
        auto value = spec.result.matchloc().to_string();
        if (value.empty()) {
            // document body is now empty; clear the json bit
            in_datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
        } else if (hasBinaryValue(spec.flags)) {
            // It was stored as a binary encoded blob, but it _could_
            // still be valid JSON...
            if (connection.getThread().isValidJson(cookie, value)) {
                in_datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
            } else {
                in_datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
            }
        } else {
            in_datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
        }
        body.reset(std::move(value));
        spec.result.clear();
    }
    return st;
}

bool SubdocExecutionContext::operate_single_doc(
        MemoryBackedBuffer* xattr,
        MemoryBackedBuffer& body,
        protocol_binary_datatype_t& doc_datatype) {
    bool modified = false;

    if (getCurrentPhase() == SubdocExecutionContext::Phase::XATTR) {
        Expects(xattr);
    } else {
        // The xattr is only provided in the xattr phase as supplying them
        // would require us to rebuild a JSON document for the key (and there
        // is no operations in the BODY phase which require the header)
        Expects(!xattr);
    }

    auto& current = getCurrentPhase() == SubdocExecutionContext::Phase::XATTR
                            ? *xattr
                            : body;

    // 2. Perform each of the operations on document.
    for (auto& op : getOperations()) {
        switch (op.traits.scope) {
        case CommandScope::SubJSON:
            if (cb::mcbp::datatype::is_json(doc_datatype)) {
                // Got JSON, perform the operation.
                op.status = operate_one_path(op, current.view);
            } else {
                // No good; need to have JSON.
                op.status = cb::mcbp::Status::SubdocDocNotJson;
            }
            break;

        case CommandScope::WholeDoc:
            op.status = operate_wholedoc(op, current.view);
            break;

        case CommandScope::AttributesAndBody:
            op.status = operate_attributes_and_body(op, xattr, body);
            break;
        }

        if (op.status == cb::mcbp::Status::Success) {
            if (traits.is_mutator) {
                modified = true;

                if (op.traits.scope != CommandScope::AttributesAndBody) {
                    // Determine how much space we now need.
                    size_t new_doc_len = 0;
                    for (auto& loc : op.result.newdoc()) {
                        new_doc_len += loc.length;
                    }

                    std::string next;
                    next.reserve(new_doc_len);
                    // TODO-PERF: We need to create a contiguous input
                    // region for the next subjson call, from the set of
                    // iovecs in the result. We can't simply write into
                    // the dynamic_buffer, as that may be the underlying
                    // storage for iovecs from the result. Ideally we'd
                    // either permit subjson to take an iovec as input, or
                    // permit subjson to take all the multipaths at once.
                    // For now we make a contiguous region in a temporary
                    // char[], and point in_doc at that.
                    for (auto& loc : op.result.newdoc()) {
                        next.insert(next.end(), loc.at, loc.at + loc.length);
                    }

                    // Copying complete - safe to delete the old temp_doc
                    // (even if it was the source of some of the newdoc
                    // iovecs).
                    current.reset(std::move(next));

                    if (op.traits.scope == CommandScope::WholeDoc) {
                        // the entire document has been replaced as part of a
                        // wholedoc op update the datatype to match

                        // don't alter context.in_datatype directly here in case
                        // we are in xattrs phase
                        if (connection.getThread().isValidJson(cookie,
                                                               current.view)) {
                            doc_datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
                        } else {
                            doc_datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
                        }
                    }
                }
            } else { // lookup
                // nothing to do.
            }
        } else {
            switch (traits.path) {
            case SubdocPath::SINGLE:
                // Failure of the (only) op stops execution
                overall_status = op.status;
                Expects(getOperations().size() == 1);
                return modified;

            case SubdocPath::MULTI:
                overall_status = cb::mcbp::Status::SubdocMultiPathFailure;
                if (traits.is_mutator) {
                    // For mutations, this stops the operation.
                    return modified;
                }
                // For lookup; an operation failing doesn't stop us
                // continuing with the rest of the operations
                // - continue with the next operation.
            }
        }
    }
    return modified;
}

cb::engine_errc SubdocExecutionContext::validate_xattr_privilege() {
    // Look at all the operations we've got in there:
    for (const auto& op : getOperations(SubdocExecutionContext::Phase::XATTR)) {
        if (cb::xattr::is_vattr(op.path)) {
            // The $document vattr doesn't require any xattr permissions,
            // but in order to get the system XATTRs included in XTOC you need
            // the system xattr privilege. Use testPrivilege instead of
            // checkPrivilege as we don't want the system to log that
            // you don't have access (we just return the user attrs)
            if (const auto [sid, cid] = cookie.getScopeAndCollection();
                op.path.rfind(cb::xattr::vattrs::XTOC, 0) == 0 &&
                cookie.testPrivilege(
                              cb::rbac::Privilege::SystemXattrRead, sid, cid)
                        .success()) {
                xtocSemantics = XtocSemantics::All;
            }
        } else {
            size_t xattr_keylen;
            is_valid_xattr_key({op.path.data(), op.path.size()}, xattr_keylen);
            std::string_view key{op.path.data(), xattr_keylen};
            if (cb::xattr::is_system_xattr(key) &&
                cookie.checkPrivilege(
                              traits.is_mutator
                                      ? cb::rbac::Privilege::SystemXattrWrite
                                      : cb::rbac::Privilege::SystemXattrRead)
                        .failed()) {
                return cb::engine_errc::no_access;
            }
        }
    }

    return cb::engine_errc::success;
}

void SubdocExecutionContext::do_xattr_delete_phase() {
    if (!do_delete_doc || !cb::mcbp::datatype::is_xattr(in_datatype)) {
        return;
    }

    // We need to remove the user keys from the Xattrs and rebuild the document

    const auto bodyoffset = cb::xattr::get_body_offset(in_doc.view);
    cb::char_buffer blob_buffer{(char*)in_doc.view.data(), bodyoffset};

    const cb::xattr::Blob xattr_blob(blob_buffer, false);

    // The backing store for the blob is currently witin the actual
    // document.. create a copy we can use for replace.
    cb::xattr::Blob copy(xattr_blob);

    // Remove the user xattrs so we're just left with system xattrs
    copy.prune_user_keys();

    rewrite_in_document(copy.finalize(), in_doc.view.substr(bodyoffset));
}

void SubdocExecutionContext::do_xattr_phase() {
    setCurrentPhase(SubdocExecutionContext::Phase::XATTR);
    if (getOperations().empty()) {
        return;
    }

    // Does the user have the permission to perform XATTRs
    auto access = validate_xattr_privilege();
    if (access != cb::engine_errc::success) {
        // Mark all as access denied
        for (auto& operation : getOperations()) {
            operation.status = cb::mcbp::Status::Eaccess;
        }

        switch (traits.path) {
        case SubdocPath::SINGLE:
            overall_status = cb::mcbp::to_status(access);
            return;

        case SubdocPath::MULTI:
            overall_status = cb::mcbp::Status::SubdocMultiPathFailure;
            return;
        }
        throw std::logic_error("do_xattr_phase: unknown SubdocPath");
    }

    auto bodyoffset = 0;

    if (cb::mcbp::datatype::is_xattr(in_datatype)) {
        bodyoffset = cb::xattr::get_body_offset(in_doc.view);
    }

    cb::char_buffer blob_buffer{(char*)in_doc.view.data(), (size_t)bodyoffset};
    xattr_buffer = cb::xattr::Blob(blob_buffer, false).to_string();

    MemoryBackedBuffer body{
            {in_doc.view.data() + bodyoffset, in_doc.view.size() - bodyoffset}};
    MemoryBackedBuffer xattr{xattr_buffer};

    for (const auto& m : {cb::xattr::macros::CAS,
                          cb::xattr::macros::SEQNO,
                          cb::xattr::macros::VALUE_CRC32C}) {
        generate_macro_padding(xattr.view, m);
    }

    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    const auto modified = operate_single_doc(&xattr, body, datatype);

    // Xattr doc should always be json
    Expects(datatype == PROTOCOL_BINARY_DATATYPE_JSON);

    // We didn't change anything in the document (or failed) so just drop
    // everything
    if (!modified || overall_status != cb::mcbp::Status::Success) {
        return;
    }

    // Time to rebuild the full document.
    cb::xattr::Blob copy;
    if (!xattr.view.empty()) {
        nlohmann::json json = nlohmann::json::parse(xattr.view);
        for (auto it = json.begin(); it != json.end(); ++it) {
            copy.set(it.key(), it.value().dump());
        }
    }

    rewrite_in_document(copy.finalize(), body.view);
    binaryEncodedStorage.clear();
}

void SubdocExecutionContext::do_body_phase() {
    setCurrentPhase(SubdocExecutionContext::Phase::Body);

    if (getOperations().empty()) {
        return;
    }

    size_t xattrsize = 0;
    MemoryBackedBuffer body{in_doc};

    if (cb::mcbp::datatype::is_xattr(in_datatype)) {
        // We shouldn't have any documents like that!
        xattrsize = cb::xattr::get_body_offset(body.view);
        body.view.remove_prefix(xattrsize);
    }

    const auto modified = operate_single_doc(nullptr, body, in_datatype);

    // We didn't change anything in the document so just drop everything
    if (!modified || overall_status != cb::mcbp::Status::Success) {
        return;
    }

    // There isn't any xattrs associated with the document. We shouldn't
    // reallocate and move things around but just reuse the temporary
    // buffer we've already created.
    if (xattrsize == 0) {
        in_doc.swap(body);
        return;
    }

    rewrite_in_document({in_doc.view.data(), xattrsize}, body.view);
}

void SubdocExecutionContext::update_statistics() {
    // Update stats. Treat all mutations as 'cmd_set', all accesses
    // as 'cmd_get', in addition to specific subdoc counters. (This
    // is mainly so we see subdoc commands in the GUI, which used
    // cmd_set / cmd_get).
    auto* ts = get_thread_stats(&cookie.getConnection());
    if (traits.is_mutator) {
        ts->cmd_subdoc_mutation++;
        ts->bytes_subdoc_mutation_total += out_doc_len;
        ts->bytes_subdoc_mutation_inserted += getOperationValueBytesTotal();
        SLAB_INCR(&cookie.getConnection(), cmd_set);
    } else {
        ts->cmd_subdoc_lookup++;
        if (cb::mcbp::isStatusSuccess(overall_status)) {
            ts->bytes_subdoc_lookup_total += in_doc.view.size();
            ts->bytes_subdoc_lookup_extracted += response_val_len;
            STATS_HIT(&cookie.getConnection(), get);
        } else {
            STATS_MISS(&cookie.getConnection(), get);
        }
    }
}
