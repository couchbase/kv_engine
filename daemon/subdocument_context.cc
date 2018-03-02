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

#include "config.h"
#include "subdocument_context.h"
#include "debug_helpers.h"
#include "mc_time.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "subdocument.h"

#include <platform/string.h>
#include <utilities/logtags.h>
#include <xattr/blob.h>

#include <utilities/string_utilities.h>
#include <gsl/gsl>
#include <iomanip>
#include <random>
#include <sstream>

SubdocCmdContext::OperationSpec::OperationSpec(SubdocCmdTraits traits_,
                                               protocol_binary_subdoc_flag flags_,
                                               cb::const_char_buffer path_)
    : SubdocCmdContext::OperationSpec::OperationSpec(traits_, flags_, path_,
                                                     {nullptr, 0}) {
}

SubdocCmdContext::OperationSpec::OperationSpec(SubdocCmdTraits traits_,
                                               protocol_binary_subdoc_flag flags_,
                                               cb::const_char_buffer path_,
                                               cb::const_char_buffer value_)
    : traits(traits_),
      flags(flags_),
      path(path_),
      value(value_),
      status(PROTOCOL_BINARY_RESPONSE_EINTERNAL) {
    if (flags & SUBDOC_FLAG_MKDIR_P) {
        traits.subdocCommand = Subdoc::Command(traits.subdocCommand |
                                               Subdoc::Command::FLAG_MKDIR_P);
    }
}

SubdocCmdContext::OperationSpec::OperationSpec(OperationSpec&& other)
    : traits(other.traits),
      flags(other.flags),
      path(std::move(other.path)),
      value(std::move(other.value)) {}

uint64_t SubdocCmdContext::getOperationValueBytesTotal() const {
    uint64_t result = 0;
    for (auto& ops : operations) {
        for (auto& op : ops) {
            result += op.value.len;
        }
    }
    return result;
}

template <typename T>
std::string SubdocCmdContext::macroToString(T macroValue) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    ss << "\"0x" << std::setw(sizeof(T) * 2) << macroValue << "\"";
    return ss.str();
}

ENGINE_ERROR_CODE SubdocCmdContext::pre_link_document(item_info& info) {
    if (do_macro_expansion) {
        auto bodyoffset = cb::xattr::get_body_offset(
            {static_cast<const char*>(info.value[0].iov_base),
             info.value[0].iov_len});

        cb::char_buffer blob_buffer{static_cast<char*>(info.value[0].iov_base),
                                    bodyoffset};

        cb::xattr::Blob xattr_blob(blob_buffer);
        auto value = xattr_blob.get(xattr_key);
        if (value.len == 0) {
            // The segment is no longer there (we may have had another
            // subdoc command which rewrote the segment where we injected
            // the macro.
            return ENGINE_SUCCESS;
        }

        // Replace the CAS
        if (std::any_of(std::begin(paddedMacros),
                        std::end(paddedMacros),
                        [](const MacroPair& m) {
                            return m.first == cb::xattr::macros::CAS;
                        })) {
            substituteMacro(cb::xattr::macros::CAS,
                            macroToString(htonll(info.cas)),
                            value);
        }

        // Replace the Seqno
        if (std::any_of(std::begin(paddedMacros),
                        std::end(paddedMacros),
                        [](const MacroPair& m) {
                            return m.first == cb::xattr::macros::SEQNO;
                        })) {
            substituteMacro(
                    cb::xattr::macros::SEQNO, macroToString(info.seqno), value);
        }
    }

    return ENGINE_SUCCESS;
}

void SubdocCmdContext::substituteMacro(cb::const_char_buffer macroName,
                                       const std::string& macroValue,
                                       cb::char_buffer& value) {
    // Do an in-place substitution of the real macro value where we
    // wrote the padded macro string.
    char* root = value.buf;
    char* end = value.buf + value.len;
    auto& macro = std::find_if(std::begin(paddedMacros),
                               std::end(paddedMacros),
                               [macroName](const MacroPair& m) {
                                   return m.first == macroName;
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

cb::const_char_buffer SubdocCmdContext::get_padded_macro(
        cb::const_char_buffer macro) {
    return std::find_if(
                   std::begin(paddedMacros),
                   std::end(paddedMacros),
                   [macro](const MacroPair& a) { return a.first == macro; })
            ->second;
}

void SubdocCmdContext::generate_macro_padding(cb::const_char_buffer payload,
                                              cb::const_char_buffer macro) {
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
        const std::string candidate = "\"" + cb::to_hex(ii) + "\"";

        for (auto& op : getOperations(Phase::XATTR)) {
            if (cb::strnstr(op.value.buf, candidate.c_str(), op.value.len)) {
                unique = false;
                break;
            }
        }

        if (unique) {
            if (cb::strnstr(payload.buf, candidate.c_str(), payload.len)) {
                unique = false;
            } else {
                paddedMacros.push_back(std::make_pair(macro, candidate));
            }
        }
    }
}

void SubdocCmdContext::setMutationSemantics(mcbp::subdoc::doc_flag docFlags) {
    if (docFlags == mcbp::subdoc::doc_flag::Add) {
        mutationSemantics = MutationSemantics::Add;
    } else if (docFlags == mcbp::subdoc::doc_flag::Mkdoc) {
        mutationSemantics = MutationSemantics::Set;
    } else {
        mutationSemantics = MutationSemantics::Replace;
    }
}

cb::const_char_buffer SubdocCmdContext::get_document_vattr() {
    if (document_vattr.empty()) {
        // @todo we can optimize this by building the json in a more efficient
        //       way, but for now just do it by using cJSON...
        unique_cJSON_ptr doc(cJSON_CreateObject());

        cJSON_AddStringToObject(
                doc.get(), "CAS", cb::to_hex(input_item_info.cas).c_str());

        cJSON_AddStringToObject(
                doc.get(),
                "vbucket_uuid",
                cb::to_hex(input_item_info.vbucket_uuid).c_str());

        cJSON_AddStringToObject(
                doc.get(), "seqno", cb::to_hex(input_item_info.seqno).c_str());

        cJSON_AddNumberToObject(doc.get(), "exptime", input_item_info.exptime);

        // The flags are kept internally in network byte order...
        cJSON_AddNumberToObject(
                doc.get(), "flags", ntohl(input_item_info.flags));

        if (mcbp::datatype::is_xattr(input_item_info.datatype)) {
            // strip off xattr
            auto bodyoffset = cb::xattr::get_body_offset(
                    {static_cast<const char*>(
                             input_item_info.value[0].iov_base),
                     input_item_info.value[0].iov_len});
            cJSON_AddNumberToObject(doc.get(),
                                    "value_bytes",
                                    input_item_info.nbytes - bodyoffset);
        } else {
            cJSON_AddNumberToObject(
                    doc.get(), "value_bytes", input_item_info.nbytes);
        }

        unique_cJSON_ptr array(cJSON_CreateArray());
        auto datatypes = split_string(
            mcbp::datatype::to_string(input_item_info.datatype), ",");
        for (const auto& d : datatypes) {
            cJSON_AddItemToArray(array.get(), cJSON_CreateString(d.c_str()));
        }

        cJSON_AddItemToObject(doc.get(), "datatype", array.release());

        cJSON_AddBoolToObject(
                doc.get(),
                "deleted",
                input_item_info.document_state == DocumentState::Deleted);

        if (input_item_info.cas_is_hlc) {
            // convert nanoseconds CAS into seconds and ensure u64 before cJSON
            // converts to a double internally.
            std::chrono::nanoseconds ns(input_item_info.cas);
            cJSON_AddStringToObject(
                    doc.get(),
                    "last_modified",
                    std::to_string(uint64_t(std::chrono::duration_cast<
                                                    std::chrono::seconds>(ns)
                                                    .count()))
                            .c_str());
        }
        unique_cJSON_ptr root(cJSON_CreateObject());
        cJSON_AddItemToObject(root.get(), "$document", doc.release());
        document_vattr = to_string(root, false);
    }

    return cb::const_char_buffer(document_vattr.data(), document_vattr.size());
}

cb::const_char_buffer SubdocCmdContext::get_xtoc_vattr() {
    if (!mcbp::datatype::is_xattr(in_datatype)) {
        xtoc_vattr = R"({"$XTOC":[]})";
        return cb::const_char_buffer(xtoc_vattr.data(), xtoc_vattr.size());
    }
    if (xtoc_vattr.empty()) {
        unique_cJSON_ptr doc(cJSON_CreateObject());

        const auto bodyoffset = cb::xattr::get_body_offset(in_doc);
        cb::char_buffer blob_buffer{const_cast<char*>(in_doc.data()),
                                    (size_t)bodyoffset};
        cb::xattr::Blob xattr_blob(blob_buffer);

        unique_cJSON_ptr array(cJSON_CreateArray());
        for (const auto& kvPair : xattr_blob) {
            bool isSystemXattr = cb::xattr::is_system_xattr(
                    const_cast<cb::const_char_buffer&>(kvPair.first));

            if (xtocSemantics == XtocSemantics::All ||
                (isSystemXattr && (xtocSemantics == XtocSemantics::System)) ||
                (!isSystemXattr && (xtocSemantics == XtocSemantics::User))) {
                cJSON_AddItemToArray(
                        array.get(),
                        cJSON_CreateString(to_string(kvPair.first).c_str()));
            }
        }

        cJSON_AddItemToObject(doc.get(), "$XTOC", array.release());
        xtoc_vattr = to_string(doc, false);
    }
    return cb::const_char_buffer(xtoc_vattr.data(), xtoc_vattr.size());
}

protocol_binary_response_status SubdocCmdContext::get_document_for_searching(
        uint64_t client_cas) {
    item_info& info = getInputItemInfo();
    auto& c = connection;

    if (!bucket_get_item_info(cookie, fetchedItem.get(), &info)) {
        LOG_WARNING("{}: Failed to get item info", c.getId());
        return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }
    if (info.cas == LOCKED_CAS) {
        // Check that item is not locked:
        if (client_cas == 0 || client_cas == LOCKED_CAS) {
            if (c.remapErrorCode(ENGINE_LOCKED_TMPFAIL) ==
                ENGINE_LOCKED_TMPFAIL) {
                return PROTOCOL_BINARY_RESPONSE_LOCKED;
            } else {
                return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
            }
        }
        // If the user *did* supply the CAS, we will validate it later on
        // when the mutation is actually applied. In any event, we don't
        // run the following branch on locked documents.
    } else if ((client_cas != 0) && client_cas != info.cas) {
        // Check CAS matches (if specified by the user).
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    }

    in_flags = info.flags;
    in_cas = client_cas ? client_cas : info.cas;
    in_doc.buf = static_cast<char*>(info.value[0].iov_base);
    in_doc.len = info.value[0].iov_len;
    in_datatype = info.datatype;
    in_document_state = info.document_state;

    if (mcbp::datatype::is_snappy(info.datatype)) {
        // Need to expand before attempting to extract from it.
        try {
            using namespace cb::compression;
            if (!inflate(Algorithm::Snappy,
                         in_doc,
                         inflated_doc_buffer)) {
                char clean_key[KEY_MAX_LENGTH + 32];
                if (buf_to_printable_buffer(clean_key,
                                            sizeof(clean_key),
                                            static_cast<const char*>(info.key),
                                            info.nkey) != -1) {
                    LOG_WARNING(
                            "<{} ERROR: Failed to determine inflated body"
                            " size. Key: '{}' may have an "
                            "incorrect datatype of COMPRESSED_JSON.",
                            c.getId(),
                            cb::logtags::tagUserData(clean_key));
                }

                return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
            }
        } catch (const std::bad_alloc&) {
            return PROTOCOL_BINARY_RESPONSE_ENOMEM;
        }

        // Update document to point to the uncompressed version in the buffer.
        in_doc = inflated_doc_buffer;
        in_datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}
