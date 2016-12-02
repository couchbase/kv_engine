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

#include <iomanip>
#include <random>
#include <sstream>
#include <daemon/xattr/blob.h>

SubdocCmdContext::OperationSpec::OperationSpec(SubdocCmdTraits traits_,
                                               protocol_binary_subdoc_flag flags_,
                                               const_char_buffer path_)
    : SubdocCmdContext::OperationSpec::OperationSpec(traits_, flags_, path_,
                                                     {nullptr, 0}) {
}

SubdocCmdContext::OperationSpec::OperationSpec(SubdocCmdTraits traits_,
                                               protocol_binary_subdoc_flag flags_,
                                               const_char_buffer path_,
                                               const_char_buffer value_)
    : traits(traits_),
      flags(flags_),
      path(path_),
      value(value_),
      status(PROTOCOL_BINARY_RESPONSE_EINTERNAL) {
    if ((flags & (SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_MKDOC))) {
        traits.command = Subdoc::Command
            (traits.command | Subdoc::Command::FLAG_MKDIR_P);
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

ENGINE_ERROR_CODE SubdocCmdContext::pre_link_document(item_info& info) {
    if (do_macro_expansion) {
        auto cas = htonll(info.cas);
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        ss << "\"0x" << std::setw(16) << cas << "\"";

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

        // Do an in-place substitution of the real cas value where we
        // wrote the padded cas string.
        uint8_t* root = value.buf;
        uint8_t* end = value.buf + value.len;
        auto* needle = padded_cas_macro.data();
        auto* needle_end = padded_cas_macro.data() + padded_cas_macro.length();

        // time to find and replace the string
        auto cas_string = ss.str();
        const size_t len = cas_string.length();

        while ((root = std::search(root, end, needle, needle_end)) != end) {
            std::copy(cas_string.data(), cas_string.data() + len, root);
            root += len;
        }
    }

    return ENGINE_SUCCESS;
}

cb::const_char_buffer SubdocCmdContext::get_padded_macro(const cb::const_char_buffer) {
    return {padded_cas_macro.data(), padded_cas_macro.length()};
}

void SubdocCmdContext::generate_cas_padding(const cb::const_char_buffer payload) {
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
                padded_cas_macro = candidate;
            }
        }
    }
}
