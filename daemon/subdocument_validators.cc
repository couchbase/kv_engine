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
