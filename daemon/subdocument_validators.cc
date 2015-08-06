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

#include "subdocument_traits.h"

static protocol_binary_response_status subdoc_validator(const void* packet, const SubdocCmdTraits traits) {
    const protocol_binary_request_subdocument *req =
            reinterpret_cast<const protocol_binary_request_subdocument*>(packet);
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
        (extlen != sizeof(uint16_t) + sizeof(uint8_t)) ||
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

    if (!traits.allow_empty_path &&
        (pathlen == 0)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status subdoc_get_validator(void* packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET>());
}

protocol_binary_response_status subdoc_exists_validator(void* packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>());
}

protocol_binary_response_status subdoc_dict_add_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>());
}

protocol_binary_response_status subdoc_dict_upsert_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>());
}

protocol_binary_response_status subdoc_delete_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>());
}

protocol_binary_response_status subdoc_replace_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>());
}

protocol_binary_response_status subdoc_array_push_last_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>());
}

protocol_binary_response_status subdoc_array_push_first_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>());
}

protocol_binary_response_status subdoc_array_insert_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT>());
}

protocol_binary_response_status subdoc_array_add_unique_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE>());
}

protocol_binary_response_status subdoc_counter_validator(void *packet) {
    return subdoc_validator(packet, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER>());
}

// Multi-path commands are a bit special - don't use the subdoc_validator<>
// for them.
protocol_binary_response_status subdoc_multi_lookup_validator(void* packet)
{
    auto req = static_cast<protocol_binary_request_subdocument_multi_lookup*>(packet);

    // 1. Check simple static values.

    // Must have at least one lookup spec; with at least a 1B path.
    const size_t minimum_body_len =
                    sizeof(protocol_binary_subdoc_multi_lookup_spec) + 1;

    if ((req->message.header.request.magic != PROTOCOL_BINARY_REQ) ||
        (req->message.header.request.keylen == 0) ||
        (req->message.header.request.extlen != 0) ||
        (req->message.header.request.bodylen < minimum_body_len) ||
        (req->message.header.request.datatype != PROTOCOL_BINARY_RAW_BYTES)) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    // 2. Check that the lookup operation specs are valid.
    const char* const body_ptr = reinterpret_cast<const char*>(packet) +
                                 sizeof(req->message.header);
    const size_t keylen = ntohs(req->message.header.request.keylen);
    const size_t bodylen = ntohl(req->message.header.request.bodylen);
    size_t body_validated = keylen;
    unsigned int path_index;
    for (path_index = 0;
         (path_index < PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS) &&
         (body_validated < bodylen);
         path_index++) {
        auto* spec = reinterpret_cast<const protocol_binary_subdoc_multi_lookup_spec*>
            (body_ptr + body_validated);

        const uint16_t pathlen = htons(spec->pathlen);

        // 2a. Check generic parameters.
        if (((spec->opcode != PROTOCOL_BINARY_CMD_SUBDOC_GET) &&
             (spec->opcode != PROTOCOL_BINARY_CMD_SUBDOC_EXISTS)) ||
            (pathlen == 0) ||
            (pathlen > SUBDOC_PATH_MAX_LENGTH)) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        // 2b. Check per-command parameters.
        switch (static_cast<protocol_binary_command>(spec->opcode))
        {
        case PROTOCOL_BINARY_CMD_SUBDOC_GET:
            {
                const SubdocCmdTraits traits =
                        get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET>();
                if ((spec->flags & ~traits.valid_flags) != 0) {
                    return PROTOCOL_BINARY_RESPONSE_EINVAL;
                }
                break;
            }
        case PROTOCOL_BINARY_CMD_SUBDOC_EXISTS:
            {
                const SubdocCmdTraits traits =
                        get_traits<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>();
                if ((spec->flags & ~traits.valid_flags) != 0) {
                    return PROTOCOL_BINARY_RESPONSE_EINVAL;
                }
                break;
            }
        default:
            return PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO;
        }

        size_t spec_len = sizeof(*spec) + pathlen;
        body_validated += spec_len;
    }

    // Only valid if we found at least one path and the validated
    // length is exactly the same as the specified length.
    if ((path_index == 0) || (body_validated != bodylen)) {
        return PROTOCOL_BINARY_RESPONSE_ERANGE;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}
