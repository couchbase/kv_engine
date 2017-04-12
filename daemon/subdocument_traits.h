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
 * Traits classes for Sub-document API commands.
 *
 * Used by both the validators and executors.
 */

#pragma once

#include "config.h"

#include <memcached/protocol_binary.h>
#include <subdoc/operations.h>

enum class SubdocPath : uint8_t {
    SINGLE,
    MULTI
};

/* Defines the scope of an operation. Whether it is on a specific path
 * (subjson) or on the whole document (wholedoc). In the wholedoc case, the
 * path can thought of as being the empty string.
 */
enum class CommandScope : uint8_t { SubJSON, WholeDoc };

/* Traits of each of the sub-document commands. These are used to build up
 * the individual implementations using generic building blocks:
 *
 *   scope: The scope of the command, subjson or wholedoc
 *   subdocCommand: The subjson API operation for this command.
 *   mcbpCommand: The mcbp command if this is a wholedoc command
 *   valid_flags: What flags are valid for this command.
 *   valid_doc_flags: What doc flags are valid for this command.
 *   request_has_value: Does the command request require a value?
 *   allow_empty_path: Is the path field allowed to be empty (zero-length)?
 *   response_has_value: Does the command response require a value?
 *   is_mutator: Does the command mutate (modify) the document?
 *   path: Whether this command on a single path or multiple paths
 */
struct SubdocCmdTraits {
    CommandScope scope;
    Subdoc::Command subdocCommand;
    protocol_binary_command mcbpCommand;
    protocol_binary_subdoc_flag valid_flags : 8;
    mcbp::subdoc::doc_flag valid_doc_flags;
    bool request_has_value;
    bool allow_empty_path;
    bool response_has_value;
    bool is_mutator;
    SubdocPath path;
};

// Additional traits for multi-path operations.
struct SubdocMultiCmdTraits {
    uint32_t min_value_len;  // Minimum size of the command value.
                             // (i.e. lookup/mutation specs)
    bool is_mutator;  // Does the command mutate (modify) the document?
};

/*
 * Dynamic lookup of a Sub-document commands' traits for the specified binary
 * protocol command.
 */
SubdocCmdTraits get_subdoc_cmd_traits(protocol_binary_command cmd);

template <protocol_binary_command CMD>
SubdocCmdTraits get_traits();

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_GET>() {
    return {CommandScope::WholeDoc,
            Subdoc::Command::INVALID,
            PROTOCOL_BINARY_CMD_GET,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ true,
            /*response_has_value*/ true,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SET>() {
    return {CommandScope::WholeDoc,
            Subdoc::Command::INVALID,
            PROTOCOL_BINARY_CMD_SET,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::Mkdoc | mcbp::subdoc::doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::GET,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ false,
            /*response_has_value*/ true,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::EXISTS,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ false,
            /*response_has_value*/ false,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::DICT_ADD,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::DICT_UPSERT,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::REMOVE,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ false,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::REPLACE,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            mcbp::subdoc::doc_flag::AccessDeleted | mcbp::subdoc::doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>() {
    using namespace mcbp::subdoc;
    return SubdocCmdTraits{CommandScope::SubJSON,
            Subdoc::Command::ARRAY_APPEND,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_EXPAND_MACROS,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>() {
    using namespace mcbp::subdoc;
    return SubdocCmdTraits{CommandScope::SubJSON,
            Subdoc::Command::ARRAY_PREPEND,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_EXPAND_MACROS,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::ARRAY_INSERT,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            mcbp::subdoc::doc_flag::AccessDeleted | mcbp::subdoc::doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::ARRAY_ADD_UNIQUE,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            /*response_has_value*/ false,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::COUNTER,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            /*response_has_value*/ true,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::GET_COUNT,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ true,
            /*response_has_value*/ true,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::INVALID,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            /*response_has_value*/ true,
            /*is_mutator*/ false,
            SubdocPath::MULTI};
}

template <>
inline SubdocCmdTraits get_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::INVALID,
            PROTOCOL_BINARY_CMD_INVALID,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::Mkdoc | mcbp::subdoc::doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            /*response_has_value*/ true,
            /*is_mutator*/ true,
            SubdocPath::MULTI};
}

template <protocol_binary_command CMD>
SubdocMultiCmdTraits get_multi_traits();

template <>
inline SubdocMultiCmdTraits get_multi_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP>() {
    // Header + 1byte path.
    return {sizeof(protocol_binary_subdoc_multi_lookup_spec) + 1,
            /*is_mutator*/false};
}

template <>
inline SubdocMultiCmdTraits get_multi_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION>() {
    // Header + 1byte path (not all mutations need a value - e.g. delete).
    return {sizeof(protocol_binary_subdoc_multi_mutation_spec) + 1,
            /*is_mutator*/true};
}
