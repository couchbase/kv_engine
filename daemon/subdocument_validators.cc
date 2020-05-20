/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/*
 * Validator functions for sub-document API commands.
 */

#include "subdocument_validators.h"

#include "connection.h"
#include "cookie.h"
#include "mcbp_validators.h"
#include "subdocument_parser.h"
#include "subdocument_traits.h"

#include "xattr/key_validator.h"
#include "xattr/utils.h"

static cb::mcbp::Status validate_basic_header_fields(Cookie& cookie) {
    auto status =
            McbpValidator::verify_header(cookie,
                                         cookie.getHeader().getExtlen(),
                                         McbpValidator::ExpectedKeyLen::NonZero,
                                         McbpValidator::ExpectedValueLen::Any,
                                         McbpValidator::ExpectedCas::Any,
                                         PROTOCOL_BINARY_RAW_BYTES);
    if (status != cb::mcbp::Status::Success) {
        return status;
    }

    if (!is_document_key_valid(cookie)) {
        return cb::mcbp::Status::Einval;
    }

    return cb::mcbp::Status::Success;
}

static inline bool validMutationSemantics(mcbp::subdoc::doc_flag a) {
    // Can't have both the Add flag and Mkdoc flag set as this doesn't mean
    // anything at the moment.
    return !(hasAdd(a) && hasMkdoc(a));
}

static bool validate_macro(const cb::const_char_buffer& value) {
    return ((value.len == cb::xattr::macros::CAS.name.len) &&
            std::memcmp(value.buf,
                        cb::xattr::macros::CAS.name.buf,
                        cb::xattr::macros::CAS.name.len) == 0) ||
           ((value.len == cb::xattr::macros::SEQNO.name.len) &&
            std::memcmp(value.buf,
                        cb::xattr::macros::SEQNO.name.buf,
                        cb::xattr::macros::SEQNO.name.len) == 0) ||
           ((value.len == cb::xattr::macros::VALUE_CRC32C.name.len) &&
            std::memcmp(value.buf,
                        cb::xattr::macros::VALUE_CRC32C.name.buf,
                        cb::xattr::macros::VALUE_CRC32C.name.len) == 0);
}

/**
 * Validate the xattr related settings that may be passed to the command.
 *
 * Check that the combination of flags is legal (you need xattr in order to
 * do macro expansion etc), and that the key conforms to the rules (and that
 * all keys refers the same xattr bulk).
 *
 * @param cookie The cookie representing the command context
 * @param mutator true if this is any of the mutator commands
 * @param flags The flag section provided
 * @param path The full path (including the key)
 * @param value The value passed (if it is a macro this must be a legal macro)
 * @param xattr_key The xattr key in use (if xattr_key.len != 0) otherwise it
 *                  the current key is stored so that we can check that the
 *                  next key refers the same key..
 * @return cb::mcbp::Status::Success if everything is correct
 */
static inline cb::mcbp::Status validate_xattr_section(
        Cookie& cookie,
        bool mutator,
        protocol_binary_subdoc_flag flags,
        cb::const_char_buffer path,
        cb::const_char_buffer value,
        cb::const_char_buffer& xattr_key) {
    if ((flags & SUBDOC_FLAG_XATTR_PATH) == 0) {
        // XATTR flag isn't set... just bail out
        if ((flags & SUBDOC_FLAG_EXPAND_MACROS)) {
            cookie.setErrorContext(
                    "EXPAND_MACROS flag requires XATTR flag to be set");
            return cb::mcbp::Status::SubdocXattrInvalidFlagCombo;
        } else {
            return cb::mcbp::Status::Success;
        }
    }

    if (!cookie.getConnection().selectedBucketIsXattrEnabled() ||
        !cookie.getConnection().isXattrEnabled()) {
        cookie.setErrorContext("Connection not XATTR enabled");
        return cb::mcbp::Status::NotSupported;
    }

    size_t key_length;
    if (!is_valid_xattr_key(path, key_length)) {
        cookie.setErrorContext("Request XATTR key invalid");
        return cb::mcbp::Status::XattrEinval;
    }

    if (path.data()[0] == '$') {
        // One may use the virtual xattrs in combination with all of the
        // other attributes - so skip key check.

        // One can't modify a virtual attribute
        if (mutator) {
            return cb::mcbp::Status::SubdocXattrCantModifyVattr;
        }
    } else {
        if (xattr_key.len == 0) {
            xattr_key.buf = path.buf;
            xattr_key.len = key_length;
        } else if (xattr_key.len != key_length ||
                   std::memcmp(xattr_key.buf, path.buf, key_length) != 0) {
            return cb::mcbp::Status::SubdocXattrInvalidKeyCombo;
        }
    }

    if (flags & SUBDOC_FLAG_EXPAND_MACROS) {
        if (!validate_macro(value)) {
            return cb::mcbp::Status::SubdocXattrUnknownMacro;
        }
    }

    return cb::mcbp::Status::Success;
}

static cb::mcbp::Status subdoc_validator(Cookie& cookie,
                                         const SubdocCmdTraits traits) {
    auto header_status = validate_basic_header_fields(cookie);
    if (header_status != cb::mcbp::Status::Success) {
        return header_status;
    }

    const auto& request = cookie.getRequest(Cookie::PacketContent::Full);
    auto extras = request.getExtdata();
    cb::mcbp::request::SubdocPayloadParser parser(extras);
    if (!parser.isValid()) {
        cookie.setErrorContext("Invalid extras section");
        return cb::mcbp::Status::Einval;
    }

    const auto pathlen = parser.getPathlen();
    if (pathlen > SUBDOC_PATH_MAX_LENGTH) {
        cookie.setErrorContext("Request path length exceeds maximum");
        return cb::mcbp::Status::Einval;
    }

    auto value = request.getValue();
    if (pathlen > value.size()) {
        cookie.setErrorContext("Path length can't exceed value");
        return cb::mcbp::Status::Einval;
    }

    cb::const_char_buffer path = {reinterpret_cast<const char*>(value.data()),
                                  pathlen};
    value = {value.data() + pathlen, value.size() - pathlen};

    // Now command-trait specific stuff:

    // valuelen should be non-zero iff the request has a value.
    if (traits.request_has_value) {
        if (value.empty()) {
            cookie.setErrorContext("Request must include value");
            return cb::mcbp::Status::Einval;
        }
    } else {
        if (!value.empty()) {
            cookie.setErrorContext("Request must not include value");
            return cb::mcbp::Status::Einval;
        }
    }

    // Check only valid flags are specified.
    const auto subdoc_flags = parser.getSubdocFlags();
    if ((subdoc_flags & ~traits.valid_flags) != 0) {
        cookie.setErrorContext("Request flags invalid");
        return cb::mcbp::Status::Einval;
    }

    const auto doc_flags = parser.getDocFlag();
    if ((doc_flags & ~traits.valid_doc_flags) != mcbp::subdoc::doc_flag::None) {
        cookie.setErrorContext("Request document flags invalid");
        return cb::mcbp::Status::Einval;
    }

    if (mcbp::subdoc::hasCreateAsDeleted(doc_flags)) {
        if (!hasAdd(doc_flags) && !hasMkdoc(doc_flags)) {
            cookie.setErrorContext("CreateAsDeleted requires Mkdoc or Add");
            return cb::mcbp::Status::Einval;
        }

        if ((subdoc_flags & SUBDOC_FLAG_XATTR_PATH) == 0) {
            cookie.setErrorContext(
                    "CreateAsDeleted does not support body path");
            return cb::mcbp::Status::Einval;
        }
    }

    // If the Add flag is set, check the cas is 0
    if (hasAdd(doc_flags) && request.getCas() != 0) {
        cookie.setErrorContext("Request with add flag must have CAS 0");
        return cb::mcbp::Status::Einval;
    }

    if (!validMutationSemantics(doc_flags)) {
        cookie.setErrorContext(
                "Request must not contain both add and mkdoc flags");
        return cb::mcbp::Status::Einval;
    }
    cb::const_char_buffer macro = {reinterpret_cast<const char*>(value.data()),
                                   value.size()};
    cb::const_char_buffer xattr_key;

    const auto status = validate_xattr_section(cookie,
                                               traits.is_mutator,
                                               subdoc_flags,
                                               path,
                                               macro,
                                               xattr_key);
    if (status != cb::mcbp::Status::Success) {
        return status;
    }

    if (!traits.allow_empty_path && (pathlen == 0)) {
        cookie.setErrorContext("Request must include path");
        return cb::mcbp::Status::Einval;
    }

    // Check that extlen is valid. For mutations can be one of 4 values
    // (depending on if an expiry or doc_flags are encoded in the request);
    // for lookups must be one of two values depending on whether doc flags
    // are encoded in the request.
    if (traits.is_mutator) {
        if ((extras.size() != SUBDOC_BASIC_EXTRAS_LEN) &&
            (extras.size() != SUBDOC_EXPIRY_EXTRAS_LEN) &&
            (extras.size() != SUBDOC_DOC_FLAG_EXTRAS_LEN) &&
            (extras.size() != SUBDOC_ALL_EXTRAS_LEN)) {
            cookie.setErrorContext("Request extras invalid");
            return cb::mcbp::Status::Einval;
        }
    } else {
        if (extras.size() != SUBDOC_BASIC_EXTRAS_LEN &&
            extras.size() != SUBDOC_DOC_FLAG_EXTRAS_LEN) {
            cookie.setErrorContext("Request extras invalid");
            return cb::mcbp::Status::Einval;
        }
    }

    return cb::mcbp::Status::Success;
}

cb::mcbp::Status subdoc_get_validator(Cookie& cookie) {
    return subdoc_validator(cookie,
                            get_traits<cb::mcbp::ClientOpcode::SubdocGet>());
}

cb::mcbp::Status subdoc_exists_validator(Cookie& cookie) {
    return subdoc_validator(cookie,
                            get_traits<cb::mcbp::ClientOpcode::SubdocExists>());
}

cb::mcbp::Status subdoc_dict_add_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocDictAdd>());
}

cb::mcbp::Status subdoc_dict_upsert_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocDictUpsert>());
}

cb::mcbp::Status subdoc_delete_validator(Cookie& cookie) {
    return subdoc_validator(cookie,
                            get_traits<cb::mcbp::ClientOpcode::SubdocDelete>());
}

cb::mcbp::Status subdoc_replace_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocReplace>());
}

cb::mcbp::Status subdoc_array_push_last_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushLast>());
}

cb::mcbp::Status subdoc_array_push_first_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushFirst>());
}

cb::mcbp::Status subdoc_array_insert_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayInsert>());
}

cb::mcbp::Status subdoc_array_add_unique_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayAddUnique>());
}

cb::mcbp::Status subdoc_counter_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocCounter>());
}

cb::mcbp::Status subdoc_get_count_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocGetCount>());
}

/**
 * Validate the multipath spec. This may be a multi mutation or lookup
 *
 * @param cookie The cookie representing the command context
 * @param ptr Pointer to the first byte of the encoded spec
 * @param traits The traits to use to validate the spec
 * @param spec_len [OUT] The number of bytes used by this spec
 * @param xattr [OUT] Did this spec reference an extended attribute
 * @param xattr_key [IN/OUT] The current extended attribute key. If its `len`
 *                  field is `0` we've not seen an extended attribute key yet
 *                  and the encoded key may be anything. If it's already set
 *                  the key `must` be the same.
 * @param doc_flags The doc flags of the multipath command
 * @param is_singleton [OUT] Does this spec require that it is the only spec
 * operating on the body.
 * @return cb::mcbp::Status::Success if everything is correct, or an
 *         error to return to the client otherwise
 */
static cb::mcbp::Status is_valid_multipath_spec(
        Cookie& cookie,
        const char* ptr,
        const SubdocMultiCmdTraits traits,
        size_t& spec_len,
        bool& xattr,
        cb::const_char_buffer& xattr_key,
        mcbp::subdoc::doc_flag doc_flags,
        bool& is_singleton) {
    // Decode the operation spec from the body. Slightly different struct
    // depending on LOOKUP/MUTATION.
    cb::mcbp::ClientOpcode opcode;
    protocol_binary_subdoc_flag flags;
    size_t headerlen;
    size_t pathlen;
    size_t valuelen;
    if (traits.is_mutator) {
        auto* spec =reinterpret_cast<const protocol_binary_subdoc_multi_mutation_spec*>
            (ptr);
        headerlen = sizeof(*spec);
        opcode = cb::mcbp::ClientOpcode(spec->opcode);
        flags = protocol_binary_subdoc_flag(spec->flags);
        pathlen = ntohs(spec->pathlen);
        valuelen = ntohl(spec->valuelen);

    } else {
        auto* spec = reinterpret_cast<const protocol_binary_subdoc_multi_lookup_spec*>
            (ptr);
        headerlen = sizeof(*spec);
        opcode = cb::mcbp::ClientOpcode(spec->opcode);
        flags = protocol_binary_subdoc_flag(spec->flags);
        pathlen = ntohs(spec->pathlen);
        valuelen = 0;
    }

    xattr = (flags & SUBDOC_FLAG_XATTR_PATH);

    SubdocCmdTraits op_traits = get_subdoc_cmd_traits(opcode);

    if (op_traits.subdocCommand == Subdoc::Command::INVALID &&
        op_traits.mcbpCommand == cb::mcbp::ClientOpcode::Invalid) {
        cookie.setErrorContext(
                "Subdoc and MCBP command must not both be invalid");
        return cb::mcbp::Status::SubdocInvalidCombo;
    }
    // Allow mutator opcodes iff the multipath command is MUTATION
    if (traits.is_mutator != op_traits.is_mutator) {
        cookie.setErrorContext(
                "Mutation opcodes only permitted for mutation commands");
        return cb::mcbp::Status::SubdocInvalidCombo;
    }

    // Check only valid flags are specified.
    if ((flags & ~op_traits.valid_flags) != 0) {
        cookie.setErrorContext("Request flags invalid");
        return cb::mcbp::Status::Einval;
    }

    if ((doc_flags & ~op_traits.valid_doc_flags) !=
        mcbp::subdoc::doc_flag::None) {
        cookie.setErrorContext("Request document flags invalid");
        return cb::mcbp::Status::Einval;
    }

    // Check path length.
    if (pathlen > SUBDOC_PATH_MAX_LENGTH) {
        cookie.setErrorContext("Request path length exceeds maximum");
        return cb::mcbp::Status::Einval;
    }

    // Check that a path isn't set for wholedoc operations
    if (op_traits.scope == CommandScope::WholeDoc && pathlen > 0) {
        cookie.setErrorContext(
                "Request must not include path for wholedoc operations");
        return cb::mcbp::Status::Einval;
    }

    cb::const_char_buffer path{ptr + headerlen, pathlen};

    cb::const_char_buffer macro{ptr + headerlen + pathlen, valuelen};

    const auto status = validate_xattr_section(cookie,
                                               traits.is_mutator,
                                               flags,
                                               path,
                                               macro,
                                               xattr_key);
    if (status != cb::mcbp::Status::Success) {
        return status;
    }

    if (!xattr) {
        if (!op_traits.allow_empty_path && (pathlen == 0)) {
            cookie.setErrorContext("Request must include path");
            return cb::mcbp::Status::Einval;
        }
    }

    // Check value length
    if (op_traits.request_has_value) {
        if (valuelen == 0) {
            cookie.setErrorContext("Request must include value");
            return cb::mcbp::Status::Einval;
        }
    } else {
        if (valuelen != 0) {
            cookie.setErrorContext("Request must not include value");
            return cb::mcbp::Status::Einval;
        }
    }

    is_singleton = (op_traits.mcbpCommand == cb::mcbp::ClientOpcode::Delete);

    spec_len = headerlen + pathlen + valuelen;
    return cb::mcbp::Status::Success;
}

// Multi-path commands are a bit special - don't use the subdoc_validator<>
// for them.
static cb::mcbp::Status subdoc_multi_validator(
        Cookie& cookie, const SubdocMultiCmdTraits traits) {

    // 1. Check simple static values.
    auto header_status = validate_basic_header_fields(cookie);
    if (header_status != cb::mcbp::Status::Success) {
        return header_status;
    }

    const auto& request = cookie.getRequest(Cookie::PacketContent::Full);
    auto extras = request.getExtdata();
    cb::mcbp::request::SubdocMultiPayloadParser parser(extras);
    if (!parser.isValid()) {
        cookie.setErrorContext("Request extras invalid");
        return cb::mcbp::Status::Einval;
    }

    // 1a. extlen can be either 0 or 1 for lookups, can be 0, 1, 4 or 5 for
    // mutations. Mutations can have expiry (4) and both mutations and lookups
    // can have doc_flags (1). We've already validated that the extras
    // size is one of the legal values, but the lookups cannot carry expiry
    if (!traits.is_mutator) {
        if ((extras.size() > 1)) {
            cookie.setErrorContext("Request extras invalid");
            return cb::mcbp::Status::Einval;
        }
    }

    const auto doc_flags = parser.getDocFlag();

    // If an add command, check that the CAS is 0:
    if (hasAdd(doc_flags) && request.getCas() != 0) {
        cookie.setErrorContext("Request with add flag must have CAS 0");
        return cb::mcbp::Status::Einval;
    }

    // Check we aren't using Add and Mkdoc together
    if (!validMutationSemantics(doc_flags)) {
        cookie.setErrorContext(
                "Request must not contain both add and mkdoc flags");
        return cb::mcbp::Status::Einval;
    }

    // 2. Check that the lookup operation specs are valid.
    //    As an "optimization" you can't mix and match the xattr and the
    //    normal paths given that they operate on different segments of
    //    the packet. Let's force the client to sort all of the xattrs
    //    operations _first_.
    const auto value = request.getValue();
    bool xattrs_allowed = true;
    const char* const body_ptr = reinterpret_cast<const char*>(value.data());

    size_t body_validated = 0;
    unsigned int path_index;

    cb::const_char_buffer xattr_key;

    bool body_commands_allowed = true;

    for (path_index = 0;
         (path_index < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) &&
         (body_validated < value.size());
         path_index++) {
        if (!body_commands_allowed) {
            cookie.setErrorContext(
                    "Request contains an invalid combination of commands");
            return cb::mcbp::Status::SubdocInvalidCombo;
        }

        size_t spec_len = 0;
        bool is_xattr;

        // true if the spec command needs to be alone in operating on the body
        bool is_isolationist;

        const auto status = is_valid_multipath_spec(cookie,
                                                    body_ptr + body_validated,
                                                    traits,
                                                    spec_len,
                                                    is_xattr,
                                                    xattr_key,
                                                    doc_flags,
                                                    is_isolationist);
        if (status != cb::mcbp::Status::Success) {
            return status;
        }

        if (xattrs_allowed) {
            xattrs_allowed = is_xattr;
        } else if (is_xattr) {
            return cb::mcbp::Status::SubdocInvalidXattrOrder;
        } else if (is_isolationist) {
            cookie.setErrorContext(
                    "Request contains an invalid combination of commands");
            return cb::mcbp::Status::SubdocInvalidCombo;
        }

        if (is_isolationist) {
            body_commands_allowed = false;
        }

        body_validated += spec_len;
    }

    // Only valid if we found at least one path and the validated
    // length is exactly the same as the specified length.
    if (path_index == 0) {
        cookie.setErrorContext("Request must contain at least one path");
        return cb::mcbp::Status::SubdocInvalidCombo;
    }
    if (body_validated != value.size()) {
        cookie.setErrorContext(
                "Request must contain at most " +
                std::to_string(PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) +
                " paths");
        return cb::mcbp::Status::SubdocInvalidCombo;
    }

    return cb::mcbp::Status::Success;
}

cb::mcbp::Status subdoc_multi_lookup_validator(Cookie& cookie) {
    return subdoc_multi_validator(
            cookie,
            get_multi_traits<cb::mcbp::ClientOpcode::SubdocMultiLookup>());
}

cb::mcbp::Status subdoc_multi_mutation_validator(Cookie& cookie) {
    return subdoc_multi_validator(
            cookie,
            get_multi_traits<cb::mcbp::ClientOpcode::SubdocMultiMutation>());
}
