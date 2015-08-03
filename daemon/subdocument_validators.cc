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

template<protocol_binary_command CMD>
static int subdoc_validator(void* packet) {
    const protocol_binary_request_subdocument *req =
            reinterpret_cast<protocol_binary_request_subdocument*>(packet);
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
        return -1;
    }

    // Now command-trait specific stuff:

    // valuelen should be non-zero iff the request has a value.
    if (cmd_traits<Cmd2Type<CMD> >::request_has_value) {
        if (valuelen == 0) {
            return -1;
        }
    } else {
        if (valuelen != 0) {
            return -1;
        }
    }

    // Check only valid flags are specified.
    if ((subdoc_flags & ~cmd_traits<Cmd2Type<CMD> >::valid_flags) != 0) {
        return -1;
    }

    if (!cmd_traits<Cmd2Type<CMD> >::allow_empty_path &&
        (pathlen == 0)) {
        return -1;
    }

    return 0;
}

int subdoc_get_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_GET>(packet);
}

int subdoc_exists_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>(packet);
}

int subdoc_dict_add_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>(packet);
}

int subdoc_dict_upsert_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>(packet);
}

int subdoc_delete_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>(packet);
}

int subdoc_replace_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>(packet);
}

int subdoc_array_push_last_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>(packet);
}

int subdoc_array_push_first_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>(packet);
}

int subdoc_array_insert_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT>(packet);
}

int subdoc_array_add_unique_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE>(packet);
}

int subdoc_counter_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER>(packet);
}

