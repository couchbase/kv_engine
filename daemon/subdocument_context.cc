/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "subdocument_context.h"

#include "debug_helpers.h"
#include "mc_time.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "settings.h"
#include "subdocument_parser.h"
#include "subdocument_validators.h"

#include <logger/logger.h>
#include <platform/crc32c.h>
#include <platform/string_hex.h>
#include <subdoc/util.h>
#include <utilities/logtags.h>
#include <utilities/string_utilities.h>
#include <xattr/blob.h>
#include <xattr/key_validator.h>
#include <gsl/gsl>
#include <iomanip>
#include <random>
#include <sstream>
#include <utility>

using namespace std::string_literals;

SubdocCmdContext::OperationSpec::OperationSpec(
        SubdocCmdTraits traits_,
        protocol_binary_subdoc_flag flags_,
        std::string path_,
        std::string value_)
    : traits(traits_),
      flags(flags_),
      path(std::move(path_)),
      value(std::move(value_)),
      status(cb::mcbp::Status::Einternal) {
    if (flags & SUBDOC_FLAG_MKDIR_P) {
        traits.subdocCommand = Subdoc::Command(traits.subdocCommand |
                                               Subdoc::Command::FLAG_MKDIR_P);
    }
}

SubdocCmdContext::OperationSpec::OperationSpec(OperationSpec&& other)
    : traits(other.traits),
      flags(other.flags),
      path(other.path),
      value(other.value) {
}

uint64_t SubdocCmdContext::getOperationValueBytesTotal() const {
    uint64_t result = 0;
    for (auto& ops : operations) {
        for (auto& op : ops) {
            result += op.value.size();
        }
    }
    return result;
}

// Debug - print details of the specified subdocument command.
static void subdoc_print_command(Connection& c,
                                 cb::mcbp::ClientOpcode cmd,
                                 const char* key,
                                 const uint16_t keylen,
                                 const char* path,
                                 const size_t pathlen,
                                 const char* value,
                                 const size_t vallen) {
    char clean_key[KEY_MAX_LENGTH + 32];
    char clean_path[SUBDOC_PATH_MAX_LENGTH];
    char clean_value[80]; // only print the first few characters of the value.
    if ((key_to_printable_buffer(clean_key,
                                 sizeof(clean_key),
                                 c.getId(),
                                 true,
                                 to_string(cb::mcbp::ClientOpcode(cmd)).c_str(),
                                 key,
                                 keylen)) &&
        (buf_to_printable_buffer(
                clean_path, sizeof(clean_path), path, pathlen))) {
        // print key, path & value if there is a value.
        if ((vallen > 0) &&
            (buf_to_printable_buffer(
                    clean_value, sizeof(clean_value), value, vallen))) {
            LOG_DEBUG("{} path:'{}' value:'{}'",
                      cb::UserDataView(clean_key),
                      cb::UserDataView(clean_path),
                      cb::UserDataView(clean_value));

        } else {
            // key & path only
            LOG_DEBUG("{} path:'{}'",
                      cb::UserDataView(clean_key),
                      cb::UserDataView(clean_path));
        }
    }
}

/*
 * Definitions
 */
void SubdocCmdContext::create_single_path_context(
        mcbp::subdoc::doc_flag doc_flags) {
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

    const bool xattr = (flags & SUBDOC_FLAG_XATTR_PATH);
    const SubdocCmdContext::Phase phase = xattr ? SubdocCmdContext::Phase::XATTR
                                                : SubdocCmdContext::Phase::Body;
    auto& ops = getOperations(phase);

    if (xattr) {
        size_t xattr_keylen;
        is_valid_xattr_key({path.data(), path.size()}, xattr_keylen);
        set_xattr_key({path.data(), xattr_keylen});
    }

    if (flags & SUBDOC_FLAG_EXPAND_MACROS) {
        do_macro_expansion = true;
    }

    // If Mkdoc or Add is specified, this implies MKDIR_P, ensure that it's set
    // here
    if (impliesMkdir_p(doc_flags)) {
        flags = flags | SUBDOC_FLAG_MKDIR_P;
    }

    // Decode as single path; add a single operation to the context.
    if (traits.request_has_value) {
        // Adjust value to move past the path.
        value.remove_prefix(pathlen);

        ops.emplace_back(
                SubdocCmdContext::OperationSpec{traits,
                                                flags,
                                                {path.data(), path.size()},
                                                {value.data(), value.size()}});
    } else {
        ops.emplace_back(SubdocCmdContext::OperationSpec{
                traits, flags, {path.data(), path.size()}});
    }

    if (impliesMkdir_p(doc_flags) && createState == DocumentState::Alive) {
        // For requests to create the key in the Alive state, set the JSON
        // of the to-be-created document root.
        jroot_type = Subdoc::Util::get_root_type(
                traits.subdocCommand, path.data(), path.size());
    }

    if (Settings::instance().getVerbose() > 1) {
        const auto keybuf = request.getKey();
        const char* key = reinterpret_cast<const char*>(keybuf.data());
        const auto keylen = gsl::narrow<uint16_t>(keybuf.size());

        subdoc_print_command(cookie.getConnection(),
                             request.getClientOpcode(),
                             key,
                             keylen,
                             path.data(),
                             path.size(),
                             value.data(),
                             value.size());
    }
}

void SubdocCmdContext::create_multi_path_context(
        mcbp::subdoc::doc_flag doc_flags) {
    // Decode each of lookup specs from the value into our command context.
    const auto& request = cookie.getRequest();
    const auto valbuf = request.getValue();
    std::string_view value{reinterpret_cast<const char*>(valbuf.data()),
                           valbuf.size()};

    size_t offset = 0;
    while (offset < value.size()) {
        cb::mcbp::ClientOpcode binprot_cmd = cb::mcbp::ClientOpcode::Invalid;
        protocol_binary_subdoc_flag flags;
        size_t headerlen;
        std::string_view path;
        std::string_view spec_value;
        if (traits.is_mutator) {
            auto* spec = reinterpret_cast<
                    const protocol_binary_subdoc_multi_mutation_spec*>(
                    value.data() + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = cb::mcbp::ClientOpcode(spec->opcode);
            flags = protocol_binary_subdoc_flag(spec->flags);
            path = {value.data() + offset + headerlen, htons(spec->pathlen)};
            spec_value = {value.data() + offset + headerlen + path.size(),
                          htonl(spec->valuelen)};

        } else {
            auto* spec = reinterpret_cast<
                    const protocol_binary_subdoc_multi_lookup_spec*>(
                    value.data() + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = cb::mcbp::ClientOpcode(spec->opcode);
            flags = protocol_binary_subdoc_flag(spec->flags);
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

        if (flags & SUBDOC_FLAG_EXPAND_MACROS) {
            do_macro_expansion = true;
        }

        const bool xattr = (flags & SUBDOC_FLAG_XATTR_PATH);
        if (xattr) {
            size_t xattr_keylen;
            is_valid_xattr_key({path.data(), path.size()}, xattr_keylen);
            std::string_view key{path.data(), xattr_keylen};
            if (!cb::xattr::is_vattr(key)) {
                set_xattr_key(key);
            }
        }

        const SubdocCmdContext::Phase phase =
                xattr ? SubdocCmdContext::Phase::XATTR
                      : SubdocCmdContext::Phase::Body;

        auto& ops = getOperations(phase);

        // Mkdoc and Add imply MKDIR_P, ensure that MKDIR_P is set
        if (impliesMkdir_p(doc_flags)) {
            flags = flags | SUBDOC_FLAG_MKDIR_P;
        }
        if (cmdTraits.mcbpCommand == cb::mcbp::ClientOpcode::Delete) {
            do_delete_doc = true;
        }
        ops.emplace_back(SubdocCmdContext::OperationSpec{
                cmdTraits,
                flags,
                {path.data(), path.size()},
                {spec_value.data(), spec_value.size()}});
        offset += headerlen + path.size() + spec_value.size();
    }

    if (Settings::instance().getVerbose() > 1) {
        const auto keybuf = request.getKey();
        const char* key = reinterpret_cast<const char*>(keybuf.data());
        const auto keylen = gsl::narrow<uint16_t>(keybuf.size());

        const char path[] = "<multipath>";
        subdoc_print_command(cookie.getConnection(),
                             request.getClientOpcode(),
                             key,
                             keylen,
                             path,
                             strlen(path),
                             value.data(),
                             value.size());
    }
}

SubdocCmdContext::SubdocCmdContext(Cookie& cookie_,
                                   const SubdocCmdTraits traits_,
                                   Vbid vbucket_,
                                   mcbp::subdoc::doc_flag doc_flags)
    : cookie(cookie_),
      connection(cookie_.getConnection()),
      traits(traits_),
      vbucket(vbucket_),
      do_allow_deleted_docs(mcbp::subdoc::hasAccessDeleted(doc_flags)),
      createState(mcbp::subdoc::hasCreateAsDeleted(doc_flags)
                          ? DocumentState::Deleted
                          : DocumentState::Alive),
      mutationSemantics(mcbp::subdoc::hasAdd(doc_flags)
                                ? MutationSemantics::Add
                                : mcbp::subdoc::hasMkdoc(doc_flags)
                                          ? MutationSemantics::Set
                                          : MutationSemantics::Replace) {
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
std::string SubdocCmdContext::macroToString(cb::xattr::macros::macro macro,
                                            T macroValue) {
    if (sizeof(T) != macro.expandedSize) {
        throw std::logic_error("macroToString: Invalid size specified for " +
                               std::string(macro.name));
    }
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    ss << "\"0x" << std::setw(sizeof(T) * 2) << macroValue << "\"";
    return ss.str();
}

ENGINE_ERROR_CODE SubdocCmdContext::pre_link_document(item_info& info) {
    if (do_macro_expansion) {
        cb::char_buffer blob_buffer{static_cast<char*>(info.value[0].iov_base),
                                    info.value[0].iov_len};

        cb::xattr::Blob xattr_blob(blob_buffer,
                                   mcbp::datatype::is_snappy(info.datatype));
        auto value = xattr_blob.get(xattr_key);
        if (value.empty()) {
            // The segment is no longer there (we may have had another
            // subdoc command which rewrote the segment where we injected
            // the macro.
            return ENGINE_SUCCESS;
        }

        // Replace the CAS
        if (containsMacro(cb::xattr::macros::CAS)) {
            substituteMacro(
                    cb::xattr::macros::CAS,
                    macroToString(cb::xattr::macros::CAS, htonll(info.cas)),
                    value);
        }

        // Replace the Seqno
        if (containsMacro(cb::xattr::macros::SEQNO)) {
            substituteMacro(cb::xattr::macros::SEQNO,
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

    return ENGINE_SUCCESS;
}

bool SubdocCmdContext::containsMacro(cb::xattr::macros::macro macro) {
    return std::any_of(std::begin(paddedMacros),
                       std::end(paddedMacros),
                       [&macro](const MacroPair& m) {
                           return m.first == macro.name;
                       });
}

void SubdocCmdContext::substituteMacro(cb::xattr::macros::macro macroName,
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
        std::copy(macroValue.data(),
                  macroValue.data() + macroValue.length(),
                  root);
        root += macroValue.length();
    }
}

std::string_view SubdocCmdContext::expand_virtual_macro(
        std::string_view macro) {
    if (macro.find(R"("${$document)") == 0) {
        return expand_virtual_document_macro(macro);
    }

    return {};
}

std::string_view SubdocCmdContext::expand_virtual_document_macro(
        std::string_view macro) {
    if (macro == R"("${$document}")") {
        // the entire document is requested!
        auto sv = get_document_vattr();
        return {sv.data(), sv.size()};
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
                std::to_string(input_item_info.revid));
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
        if (mcbp::datatype::is_xattr(in_datatype)) {
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
                mcbp::datatype::to_string(input_item_info.datatype), ",");
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

std::string_view SubdocCmdContext::get_padded_macro(std::string_view macro) {
    auto iter = std::find_if(
            std::begin(paddedMacros),
            std::end(paddedMacros),
            [macro](const MacroPair& a) { return a.first == macro; });
    if (iter == paddedMacros.end()) {
        return {};
    }

    return iter->second;
}

void SubdocCmdContext::generate_macro_padding(std::string_view payload,
                                              cb::xattr::macros::macro macro) {
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
            break;
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

std::string_view SubdocCmdContext::get_document_vattr() {
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
        if (mcbp::datatype::is_xattr(in_datatype)) {
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
            mcbp::datatype::to_string(input_item_info.datatype), ",");
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

    return std::string_view(document_vattr.data(), document_vattr.size());
}

std::string_view SubdocCmdContext::get_vbucket_vattr() {
    if (vbucket_vattr.empty()) {
        auto hlc = connection.getBucketEngine().getVBucketHlcNow(vbucket);
        using namespace nlohmann;
        std::string mode =
                (hlc.mode == cb::HlcTime::Mode::Real) ? "real"s : "logical"s;
        json root = {{"$vbucket",
                      {{"HLC",
                        {{"now", std::to_string(hlc.now.count())},
                         {"mode", mode}}}}}};
        vbucket_vattr = root.dump();
    }

    return vbucket_vattr;
}

std::string_view SubdocCmdContext::get_xtoc_vattr() {
    if (!mcbp::datatype::is_xattr(in_datatype)) {
        xtoc_vattr = R"({"$XTOC":[]})";
        return std::string_view(xtoc_vattr.data(), xtoc_vattr.size());
    }
    if (xtoc_vattr.empty()) {
        const auto bodyoffset = cb::xattr::get_body_offset(in_doc.view);
        cb::char_buffer blob_buffer{const_cast<char*>(in_doc.view.data()),
                                    (size_t)bodyoffset};
        cb::xattr::Blob xattr_blob(blob_buffer,
                                   mcbp::datatype::is_snappy(in_datatype));

        nlohmann::json arr;
        for (const auto [key, value] : xattr_blob) {
            (void)value;
            bool isSystemXattr = cb::xattr::is_system_xattr(key);

            if (xtocSemantics == XtocSemantics::All ||
                (isSystemXattr && (xtocSemantics == XtocSemantics::System)) ||
                (!isSystemXattr && (xtocSemantics == XtocSemantics::User))) {
                arr.push_back(key);
            }
        }

        nlohmann::json doc;
        doc["$XTOC"] = arr;
        xtoc_vattr = doc.dump();
    }
    return std::string_view(xtoc_vattr.data(), xtoc_vattr.size());
}

cb::mcbp::Status SubdocCmdContext::get_document_for_searching(
        uint64_t client_cas) {
    item_info& info = getInputItemInfo();
    auto& c = connection;

    if (!bucket_get_item_info(connection, fetchedItem.get(), &info)) {
        LOG_WARNING("{}: Failed to get item info", c.getId());
        return cb::mcbp::Status::Einternal;
    }
    if (info.cas == LOCKED_CAS) {
        // Check that item is not locked:
        if (client_cas == 0 || client_cas == LOCKED_CAS) {
            if (c.remapErrorCode(ENGINE_LOCKED_TMPFAIL) ==
                ENGINE_LOCKED_TMPFAIL) {
                return cb::mcbp::Status::Locked;
            } else {
                return cb::mcbp::Status::Etmpfail;
            }
        }
        // If the user *did* supply the CAS, we will validate it later on
        // when the mutation is actually applied. In any event, we don't
        // run the following branch on locked documents.
    } else if ((client_cas != 0) && client_cas != info.cas) {
        // Check CAS matches (if specified by the user).
        return cb::mcbp::Status::KeyEexists;
    }

    in_flags = info.flags;
    in_cas = client_cas ? client_cas : info.cas;
    in_doc = MemoryBackedBuffer{{static_cast<char*>(info.value[0].iov_base),
                                 info.value[0].iov_len}};
    in_datatype = info.datatype;
    in_document_state = info.document_state;

    if (mcbp::datatype::is_snappy(info.datatype)) {
        // Need to expand before attempting to extract from it.
        try {
            using namespace cb::compression;
            if (!inflate(Algorithm::Snappy, in_doc.view, inflated_doc_buffer)) {
                char clean_key[KEY_MAX_LENGTH + 32];
                if (buf_to_printable_buffer(
                            clean_key,
                            sizeof(clean_key),
                            reinterpret_cast<const char*>(info.key.data()),
                            info.key.size())) {
                    LOG_WARNING(
                            "<{} ERROR: Failed to determine inflated body"
                            " size. Key: '{}' may have an "
                            "incorrect datatype of COMPRESSED_JSON.",
                            c.getId(),
                            cb::UserDataView(clean_key));
                }

                return cb::mcbp::Status::Einternal;
            }
        } catch (const std::bad_alloc&) {
            return cb::mcbp::Status::Enomem;
        }

        // Update document to point to the uncompressed version in the buffer.
        in_doc = MemoryBackedBuffer{inflated_doc_buffer};
        in_datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    }

    return cb::mcbp::Status::Success;
}

uint32_t SubdocCmdContext::computeValueCRC32C() {
    std::string_view value;
    if (mcbp::datatype::is_xattr(in_datatype)) {
        // Note: in the XAttr naming, body/value excludes XAttrs
        value = cb::xattr::get_body(in_doc.view);
    } else {
        value = in_doc.view;
    }
    return crc32c(reinterpret_cast<const unsigned char*>(value.data()),
                  value.size(),
                  0 /*crc_in*/);
}
