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

#include "connection_mcbp.h"
#include "subdocument_validators.h"
#include "subdocument_traits.h"

#include "xattr/key_validator.h"

static bool validate_macro(const cb::const_byte_buffer& value) {
    static cb::const_byte_buffer doc_cas{(const uint8_t*)"\"${Mutation.CAS}\"",
                                         17};
    return ((value.len == doc_cas.len) &&
            std::memcmp(value.buf, doc_cas.buf, doc_cas.len) == 0);
}

/**
 * Validate the xattr related settings that may be passed to the command.
 *
 * Check that the combination of flags is legal (you need xattr in order to
 * do macro expansion etc), and that the key conforms to the rules (and that
 * all keys refers the same xattr bulk).
 *
 * @param flags The flag section provided
 * @param path The full path (including the key)
 * @param value The value passed (if it is a macro this must be a legal macro)
 * @param xattr_key The xattr key in use (if xattr_key.len != 0) otherwise it
 *                  the current key is stored so that we can check that the
 *                  next key refers the same key..
 * @return PROTOCOL_BINARY_RESPONSE_SUCCESS if everything is correct
 */
static inline protocol_binary_response_status validate_xattr_section(
                                          protocol_binary_subdoc_flag flags,
                                          cb::const_byte_buffer path,
                                          cb::const_byte_buffer value,
                                          cb::const_byte_buffer& xattr_key) {
    if ((flags & SUBDOC_FLAG_XATTR_PATH) == 0) {
        // XATTR flag isn't set... just bail out
        if ((flags & SUBDOC_FLAG_EXPAND_MACROS) ||
                 (flags & SUBDOC_FLAG_ACCESS_DELETED)) {
            return PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_INVALID_FLAG_COMBO;
        } else {
            return PROTOCOL_BINARY_RESPONSE_SUCCESS;
        }
    }

    size_t key_length;
    if (!is_valid_xattr_key(path, key_length)) {
        return PROTOCOL_BINARY_RESPONSE_XATTR_EINVAL;
    }

    if (xattr_key.len == 0) {
        xattr_key.buf = path.buf;
        xattr_key.len = key_length;
    } else if (xattr_key.len != key_length ||
               std::memcmp(xattr_key.buf, path.buf, key_length) != 0) {
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_INVALID_KEY_COMBO;
    }

    if (flags & SUBDOC_FLAG_EXPAND_MACROS) {
        if (!validate_macro(value)) {
            return PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_UNKNOWN_MACRO;
        }
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static protocol_binary_response_status subdoc_validator(const Cookie& cookie, const SubdocCmdTraits traits) {
    auto req = reinterpret_cast<const protocol_binary_request_subdocument*>(McbpConnection::getPacket(cookie));
    const protocol_binary_request_header* header = &req->message.header;
    // Extract the various fields from the header.
    const uint16_t keylen = ntohs(header->request.keylen);
    const uint8_t extlen = header->request.extlen;
    const uint32_t bodylen = ntohl(header->request.bodylen);
    const protocol_binary_subdoc_flag subdoc_flags =
            static_cast<protocol_binary_subdoc_flag>(req->message.extras.subdoc_flags);
    const uint16_t pathlen = ntohs(req->message.extras.pathlen);
    const uint32_t valuelen = bodylen - keylen - extlen - pathlen;

    if ((header->request.magic != PROTOCOL_BINARY_REQ) ||
        (keylen == 0) ||
        (pathlen > SUBDOC_PATH_MAX_LENGTH) ||
        (header->request.datatype != PROTOCOL_BINARY_RAW_BYTES)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    // Now command-trait specific stuff:

    // valuelen should be non-zero iff the request has a value.
    if (traits.request_has_value) {
        if (valuelen == 0) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    } else {
        if (valuelen != 0) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    }

    // Check only valid flags are specified.
    if ((subdoc_flags & ~traits.valid_flags) != 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    cb::const_byte_buffer path{req->message.header.bytes +
                               sizeof(req->message.header.bytes) +
                               keylen + extlen,
                               pathlen};
    cb::const_byte_buffer macro{req->message.header.bytes +
                                sizeof(req->message.header.bytes) +
                                keylen + extlen + pathlen,
                                valuelen};
    cb::const_byte_buffer xattr_key;

    const auto status = validate_xattr_section(subdoc_flags, path, macro,
                                               xattr_key);
    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return status;
    }

    if (!traits.allow_empty_path && (pathlen == 0)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    // Check that extlen is valid. For mutations can be one of two values
    // (depending on if an expiry is encoded in the request); for lookups must
    // be a fixed value.
    if (traits.is_mutator) {
        if ((extlen != SUBDOC_BASIC_EXTRAS_LEN) &&
            (extlen != SUBDOC_EXPIRY_EXTRAS_LEN)) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    } else {
        if (extlen != SUBDOC_BASIC_EXTRAS_LEN) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status subdoc_get_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET>());
}

protocol_binary_response_status subdoc_exists_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>());
}

protocol_binary_response_status subdoc_dict_add_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>());
}

protocol_binary_response_status subdoc_dict_upsert_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>());
}

protocol_binary_response_status subdoc_delete_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>());
}

protocol_binary_response_status subdoc_replace_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>());
}

protocol_binary_response_status subdoc_array_push_last_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>());
}

protocol_binary_response_status subdoc_array_push_first_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>());
}

protocol_binary_response_status subdoc_array_insert_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT>());
}

protocol_binary_response_status subdoc_array_add_unique_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE>());
}

protocol_binary_response_status subdoc_counter_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER>());
}

protocol_binary_response_status subdoc_get_count_validator(const Cookie& cookie) {
    return subdoc_validator(cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT>());
}

/**
 * Validate the multipath spec. This may be a multi mutation or lookup
 *
 * @param ptr Pointer to the first byte of the encoded spec
 * @param traits The traits to use to validate the spec
 * @param spec_len [OUT] The number of bytes used by this spec
 * @param xattr [OUT] Did this spec reference an extended attribute
 * @param xattr_key [IN/OUT] The current extended attribute key. If its `len`
 *                  field is `0` we've not seen an extended attribute key yet
 *                  and the encoded key may be anything. If it's already set
 *                  the key `must` be the same.
 * @return PROTOCOL_BINARY_RESPONSE_SUCCESS if everything is correct, or an
 *         error to return to the client otherwise
 */
static protocol_binary_response_status is_valid_multipath_spec(const char* ptr,
                                                               const SubdocMultiCmdTraits traits,
                                                               size_t& spec_len,
                                                               bool& xattr,
                                                               cb::const_byte_buffer& xattr_key) {

    // Decode the operation spec from the body. Slightly different struct
    // depending on LOOKUP/MUTATION.
    protocol_binary_command opcode;
    protocol_binary_subdoc_flag flags;
    size_t headerlen;
    size_t pathlen;
    size_t valuelen;
    if (traits.is_mutator) {
        auto* spec =reinterpret_cast<const protocol_binary_subdoc_multi_mutation_spec*>
            (ptr);
        headerlen = sizeof(*spec);
        opcode = protocol_binary_command(spec->opcode);
        flags = protocol_binary_subdoc_flag(spec->flags);
        pathlen = ntohs(spec->pathlen);
        valuelen = ntohl(spec->valuelen);

    } else {
        auto* spec = reinterpret_cast<const protocol_binary_subdoc_multi_lookup_spec*>
            (ptr);
        headerlen = sizeof(*spec);
        opcode = protocol_binary_command(spec->opcode);
        flags = protocol_binary_subdoc_flag(spec->flags);
        pathlen = ntohs(spec->pathlen);
        valuelen = 0;
    }

    xattr = (flags & SUBDOC_FLAG_XATTR_PATH);

    SubdocCmdTraits op_traits = get_subdoc_cmd_traits(opcode);

    if (op_traits.command == Subdoc::Command::INVALID) {
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO;
    }
    // Allow mutator opcodes iff the multipath command is MUTATION
    if (traits.is_mutator != op_traits.is_mutator) {
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO;
    }

    // Check only valid flags are specified.
    if ((flags & ~op_traits.valid_flags) != 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    // Check path length.
    if (pathlen > SUBDOC_PATH_MAX_LENGTH) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    cb::const_byte_buffer path{reinterpret_cast<const uint8_t*>(ptr) + headerlen,
                               pathlen};

    cb::const_byte_buffer macro{reinterpret_cast<const uint8_t*>(ptr) +
                                headerlen + pathlen,
                                valuelen};

    const auto status = validate_xattr_section(flags, path, macro, xattr_key);
    if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return status;
    }

    if (!xattr) {
        if (!op_traits.allow_empty_path && (pathlen == 0)) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    }

    // Check value length
    if (op_traits.request_has_value) {
        if (valuelen == 0) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    } else {
        if (valuelen != 0) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    }

    spec_len = headerlen + pathlen + valuelen;
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}


// Multi-path commands are a bit special - don't use the subdoc_validator<>
// for them.
static protocol_binary_response_status subdoc_multi_validator(const Cookie& cookie,
                                                              const SubdocMultiCmdTraits traits)
{
    auto req = static_cast<protocol_binary_request_header*>(McbpConnection::getPacket(cookie));

    // 1. Check simple static values.

    // Must have at least one lookup spec
    const size_t minimum_body_len = ntohs(req->request.keylen) + req->request.extlen + traits.min_value_len;

    if ((req->request.magic != PROTOCOL_BINARY_REQ) ||
        (req->request.keylen == 0) ||
        (req->request.bodylen < minimum_body_len) ||
        (req->request.datatype != PROTOCOL_BINARY_RAW_BYTES)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    // 1a. extlen must be zero for lookups, can be 0 or 4 for mutations
    // (to specify expiry).
    if (traits.is_mutator) {
        if ((req->request.extlen != 0) &&
            (req->request.extlen != sizeof(uint32_t))) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    } else {
        if (req->request.extlen != 0) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    }

    // 2. Check that the lookup operation specs are valid.
    //    As an "optimization" you can't mix and match the xattr and the
    //    normal paths given that they operate on different segments of
    //    the packet. Let's force the client to sort all of the xattrs
    //    operations _first_.
    bool xattrs_allowed = true;
    const char* const body_ptr = reinterpret_cast<const char*>(McbpConnection::getPacket(cookie)) +
                                 sizeof(*req);
    const size_t keylen = ntohs(req->request.keylen);
    const size_t bodylen = ntohl(req->request.bodylen);
    size_t body_validated = keylen + req->request.extlen;
    unsigned int path_index;

    cb::const_byte_buffer xattr_key;

    for (path_index = 0;
         (path_index < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) &&
         (body_validated < bodylen);
         path_index++) {

        size_t spec_len = 0;
        bool is_xattr;
        const auto status = is_valid_multipath_spec(body_ptr + body_validated,
                                                    traits, spec_len,
                                                    is_xattr, xattr_key);
        if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            return status;
        }

        if (xattrs_allowed) {
            xattrs_allowed = is_xattr;
        } else if (is_xattr) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        body_validated += spec_len;

    }

    // Only valid if we found at least one path and the validated
    // length is exactly the same as the specified length.
    if ((path_index == 0) || (body_validated != bodylen)) {
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status subdoc_multi_lookup_validator(const Cookie& cookie) {
    return subdoc_multi_validator(cookie, get_multi_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP>());
}

protocol_binary_response_status subdoc_multi_mutation_validator(const Cookie& cookie) {
    return subdoc_multi_validator(cookie, get_multi_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION>());
}
