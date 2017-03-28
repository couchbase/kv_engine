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
#include "subdocument.h"
#include "subdocument_context.h"

#include <xattr/blob.h>

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
    if ((flags & (SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_MKDOC))) {
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

        cb::byte_buffer blob_buffer{
            static_cast<uint8_t*>(info.value[0].iov_base),
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
                                       cb::byte_buffer& value) {
    // Do an in-place substitution of the real macro value where we
    // wrote the padded macro string.
    uint8_t* root = value.buf;
    uint8_t* end = value.buf + value.len;
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
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        ss << "\"0x" << std::setw(16) << ii << "\"";
        const std::string candidate = ss.str();

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
