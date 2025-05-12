/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
#include <daemon/settings.h>
#include <utilities/string_utilities.h>
#include <xattr/key_validator.h>
#include <xattr/utils.h>

static cb::mcbp::Status validate_basic_header_fields(Cookie& cookie) {
    auto status =
            McbpValidator::verify_header(cookie,
                                         cookie.getHeader().getExtlen(),
                                         McbpValidator::ExpectedKeyLen::NonZero,
                                         McbpValidator::ExpectedValueLen::Any,
                                         McbpValidator::ExpectedCas::Any,
                                         McbpValidator::GeneratesDocKey::Yes,
                                         PROTOCOL_BINARY_RAW_BYTES);
    if (status != cb::mcbp::Status::Success) {
        return status;
    }

    return cb::mcbp::Status::Success;
}

static bool validMutationSemantics(cb::mcbp::subdoc::DocFlag a) {
    // Can't have both the Add flag and Mkdoc flag set as this doesn't mean
    // anything at the moment.
    return !(hasAdd(a) && hasMkdoc(a));
}

static cb::mcbp::Status validate_macro(std::string_view value) {
    using namespace cb::xattr::macros;
    using cb::mcbp::Status;

    if ((value == CAS.name) || (value == SEQNO.name) ||
        (value == VALUE_CRC32C.name)) {
        return Status::Success;
    }

    // is it any of our virtual macros?
    if (value.starts_with(R"("${$document)") && value.ends_with(R"(}")")) {
        return Status::Success;
    }

    return Status::SubdocXattrUnknownMacro;
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
 * @return cb::mcbp::Status::Success if everything is correct
 */
static cb::mcbp::Status validate_xattr_section(Cookie& cookie,
                                               bool mutator,
                                               cb::mcbp::subdoc::PathFlag flags,
                                               std::string_view path,
                                               std::string_view value) {
    if (!hasXattrPath(flags)) {
        if (hasExpandedMacros(flags)) {
            cookie.setErrorContext(
                    "EXPAND_MACROS flag requires XATTR flag to be set");
            return cb::mcbp::Status::SubdocXattrInvalidFlagCombo;
        }

        if (hasBinaryValue(flags)) {
            cookie.setErrorContext(
                    "BINARY_VALUE flag requires XATTR flag to be set");
            return cb::mcbp::Status::SubdocXattrInvalidFlagCombo;
        }

        return cb::mcbp::Status::Success;
    }

    if (hasBinaryValue(flags) && hasExpandedMacros(flags)) {
        cookie.setErrorContext(
                "BINARY_VALUE can't be used together with EXPAND_MACROS");
        // the value can't be binary _AND_ contain macros to expand
        return cb::mcbp::Status::SubdocXattrInvalidFlagCombo;
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

    if (cb::xattr::is_vattr(path) && mutator) {
        return cb::mcbp::Status::SubdocXattrCantModifyVattr;
    }

    if (const auto start = value.find("cb-content-base64-encoded:");
        start != std::string_view::npos) {
        // validate that it is correctly encoded
        std::string_view view;
        if (start == 0 || (start == 1 && value.front() == '"')) {
            // view is the entire view
            view = value;
        } else {
            view = value;
            view.remove_prefix(start);
            auto end = view.find('"');
            if (end == std::string_view::npos) {
                cookie.setErrorContext("Incorrectly encoded binary value");
                return cb::mcbp::Status::XattrEinval;
            }
            view = view.substr(0, end);
        }

        try {
            base64_decode_value(view);
        } catch (const std::exception& e) {
            cookie.setErrorContext(e.what());
            return cb::mcbp::Status::XattrEinval;
        }
    }

    return hasExpandedMacros(flags) ? validate_macro(value)
                                    : cb::mcbp::Status::Success;
}

static cb::mcbp::Status subdoc_validator(Cookie& cookie,
                                         const SubdocCmdTraits traits) {
    auto header_status = validate_basic_header_fields(cookie);
    if (header_status != cb::mcbp::Status::Success) {
        return header_status;
    }

    const auto& request = cookie.getRequest();
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

    auto value = request.getValueString();
    if (pathlen > value.size()) {
        cookie.setErrorContext("Path length can't exceed value");
        return cb::mcbp::Status::Einval;
    }

    std::string_view path = {value.data(), pathlen};
    value.remove_prefix(pathlen);

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
    if ((subdoc_flags & ~traits.valid_flags) !=
        cb::mcbp::subdoc::PathFlag::None) {
        cookie.setErrorContext("Request flags invalid");
        return cb::mcbp::Status::Einval;
    }

    // Check all mandatory flags are specified
    if ((subdoc_flags & traits.mandatory_flags) != traits.mandatory_flags) {
        cookie.setErrorContext("Request missing mandatory flags");
        return cb::mcbp::Status::Einval;
    }

    const auto doc_flags = parser.getDocFlag();
    if ((doc_flags & ~traits.valid_doc_flags) !=
        cb::mcbp::subdoc::DocFlag::None) {
        cookie.setErrorContext("Request document flags invalid");
        return cb::mcbp::Status::Einval;
    }

    if (cb::mcbp::subdoc::hasCreateAsDeleted(doc_flags)) {
        if (!hasAdd(doc_flags) && !hasMkdoc(doc_flags)) {
            cookie.setErrorContext("CreateAsDeleted requires Mkdoc or Add");
            return cb::mcbp::Status::Einval;
        }

        if (!hasXattrPath(subdoc_flags)) {
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

    const auto status = validate_xattr_section(
            cookie, traits.is_mutator, subdoc_flags, path, value);
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
    cb::mcbp::request::SubdocPayloadParser payload_parser(extras);
    if (traits.is_mutator) {
        if (!payload_parser.isValidMutation()) {
            cookie.setErrorContext("Request extras invalid");
            return cb::mcbp::Status::Einval;
        }
    } else {
        if (!payload_parser.isValidLookup()) {
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

cb::mcbp::Status subdoc_replace_body_with_xattr_validator(Cookie& cookie) {
    return subdoc_validator(
            cookie,
            get_traits<cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr>());
}

/**
 * Validate the multipath spec. This may be a multi mutation or lookup
 *
 * @param cookie The cookie representing the command context
 * @param blob The remainder of the unparsed data
 * @param traits The traits to use to validate the spec
 * @param spec_len [OUT] The number of bytes used by this spec
 * @param xattr [OUT] Did this spec reference an extended attribute
 * @param doc_flags The doc flags of the multipath command
 * @param is_singleton [OUT] Does this spec require that it is the only spec
 * operating on the body.
 * @return cb::mcbp::Status::Success if everything is correct, or an
 *         error to return to the client otherwise
 */
static cb::mcbp::Status is_valid_multipath_spec(
        Cookie& cookie,
        std::string_view blob,
        const SubdocMultiCmdTraits traits,
        size_t& spec_len,
        bool& xattr,
        cb::mcbp::subdoc::DocFlag doc_flags,
        bool& is_singleton) {
    // Decode the operation spec from the body. Slightly different struct
    // depending on LOOKUP/MUTATION.
    cb::mcbp::ClientOpcode opcode;
    cb::mcbp::subdoc::PathFlag flags;
    size_t headerlen;
    size_t pathlen;
    size_t valuelen;
    if (traits.is_mutator) {
        auto* spec = reinterpret_cast<
                const protocol_binary_subdoc_multi_mutation_spec*>(blob.data());
        headerlen = sizeof(*spec);
        if (headerlen > blob.size()) {
            cookie.setErrorContext("Multi mutation spec truncated");
            return cb::mcbp::Status::Einval;
        }
        opcode = cb::mcbp::ClientOpcode(spec->opcode);
        flags = cb::mcbp::subdoc::PathFlag(spec->flags);
        pathlen = ntohs(spec->pathlen);
        valuelen = ntohl(spec->valuelen);

        if (headerlen + pathlen + valuelen > blob.size()) {
            cookie.setErrorContext("Multi mutation path and value truncated");
            return cb::mcbp::Status::Einval;
        }
    } else {
        auto* spec = reinterpret_cast<
                const protocol_binary_subdoc_multi_lookup_spec*>(blob.data());
        headerlen = sizeof(*spec);
        if (headerlen > blob.size()) {
            cookie.setErrorContext("Multi lookup spec truncated");
            return cb::mcbp::Status::Einval;
        }
        opcode = cb::mcbp::ClientOpcode(spec->opcode);
        flags = cb::mcbp::subdoc::PathFlag(spec->flags);
        pathlen = ntohs(spec->pathlen);
        valuelen = 0;
        if (headerlen + pathlen > blob.size()) {
            cookie.setErrorContext("Multi lookup path truncated");
            return cb::mcbp::Status::Einval;
        }
    }

    xattr = hasXattrPath(flags);

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
    if ((flags & ~op_traits.valid_flags) != cb::mcbp::subdoc::PathFlag::None) {
        cookie.setErrorContext("Request flags invalid");
        return cb::mcbp::Status::Einval;
    }

    // Check all mandatory flags are specified
    if ((flags & op_traits.mandatory_flags) != op_traits.mandatory_flags) {
        cookie.setErrorContext("Request missing mandatory flags");
        return cb::mcbp::Status::Einval;
    }

    if ((doc_flags & ~op_traits.valid_doc_flags) !=
        cb::mcbp::subdoc::DocFlag::None) {
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

    std::string_view path{blob.data() + headerlen, pathlen};
    std::string_view macro{blob.data() + headerlen + pathlen, valuelen};

    const auto status = validate_xattr_section(
            cookie, traits.is_mutator, flags, path, macro);
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

    const auto& request = cookie.getRequest();
    auto extras = request.getExtdata();
    cb::mcbp::request::SubdocMultiPayloadParser parser(extras);
    if (!parser.isValid()) {
        cookie.setErrorContext("Request extras invalid");
        return cb::mcbp::Status::Einval;
    }

    if (traits.is_mutator) {
        if (!parser.isValidMutation()) {
            cookie.setErrorContext("Request extras invalid");
            return cb::mcbp::Status::Einval;
        }
    } else {
        if (!parser.isValidLookup()) {
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

    if (cb::mcbp::subdoc::hasReviveDocument(doc_flags)) {
        if (!cb::mcbp::subdoc::hasAccessDeleted(doc_flags)) {
            cookie.setErrorContext(
                    "ReviveDocument can't be used without AccessDeleted");
            return cb::mcbp::Status::Einval;
        }
        if (cb::mcbp::subdoc::hasCreateAsDeleted(doc_flags)) {
            cookie.setErrorContext(
                    "ReviveDocument can't be used with CreateAsDeleted");
            return cb::mcbp::Status::Einval;
        }
    }

    // 2. Check that the lookup operation specs are valid.
    //    As an "optimization" you can't mix and match the xattr and the
    //    normal paths given that they operate on different segments of
    //    the packet. Let's force the client to sort the xattrs
    //    operations _first_.
    const auto value = request.getValue();
    bool xattrs_allowed = true;
    const char* const body_ptr = reinterpret_cast<const char*>(value.data());

    size_t body_validated = 0;
    unsigned int path_index;

    bool body_commands_allowed = true;

    const auto max_paths = Settings::instance().getSubdocMultiMaxPaths();

    for (path_index = 0;
         (path_index < max_paths) && (body_validated < value.size());
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

        const auto status = is_valid_multipath_spec(
                cookie,
                {body_ptr + body_validated, value.size() - body_validated},
                traits,
                spec_len,
                is_xattr,
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
        cookie.setErrorContext("Request must contain at most " +
                               std::to_string(max_paths) + " paths");
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
