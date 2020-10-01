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
enum class CommandScope : uint8_t {
    /// Using SubJSON to update the document
    SubJSON,
    /// Operate on the whole document (the path can though of as being empty
    /// string)
    WholeDoc,
    /// Some commands (currently only copy) allows for modifications on both
    /// XATTRS and the document body
    AttributesAndBody
};

/**
 * What kind of response does a command return?
 */
enum class ResponseValue : uint8_t {
    None, // No response is returned,
    JSON, // Returns response in JSON
    Binary, // Returns response in non-JSON (binary).
    FromDocument // Returns reponse; datatype is of the targetted document.
};

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
 *   response_value: Does the command response have a value, and if so what type?
 *   is_mutator: Does the command mutate (modify) the document?
 *   path: Whether this command on a single path or multiple paths
 */
struct SubdocCmdTraits {
    CommandScope scope;
    Subdoc::Command subdocCommand;
    cb::mcbp::ClientOpcode mcbpCommand;
    protocol_binary_subdoc_flag valid_flags : 8;
    protocol_binary_subdoc_flag mandatory_flags : 8;
    mcbp::subdoc::doc_flag valid_doc_flags;
    bool request_has_value;
    bool allow_empty_path;
    ResponseValue response_type;
    bool is_mutator;
    SubdocPath path;

    bool responseHasValue() const {
        return response_type != ResponseValue::None;
    }

    /// Returns the datatype to encode into a mcbp response.
    cb::mcbp::Datatype responseDatatype(
            protocol_binary_datatype_t docDatatype) const;
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
SubdocCmdTraits get_subdoc_cmd_traits(cb::mcbp::ClientOpcode cmd);

template <cb::mcbp::ClientOpcode CMD>
SubdocCmdTraits get_traits();

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::Get>() {
    return {CommandScope::WholeDoc,
            Subdoc::Command::INVALID,
            cb::mcbp::ClientOpcode::Get,
            SUBDOC_FLAG_NONE,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ true,
            ResponseValue::FromDocument,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::Set>() {
    return {CommandScope::WholeDoc,
            Subdoc::Command::INVALID,
            cb::mcbp::ClientOpcode::Set,
            SUBDOC_FLAG_NONE,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::Mkdoc | mcbp::subdoc::doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::Delete>() {
    return {CommandScope::WholeDoc,
            Subdoc::Command::INVALID,
            cb::mcbp::ClientOpcode::Delete,
            SUBDOC_FLAG_NONE,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ true,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocGet>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::GET,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ false,
            ResponseValue::JSON,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocExists>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::EXISTS,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ false,
            ResponseValue::None,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocDictAdd>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::DICT_ADD,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            SUBDOC_FLAG_NONE,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add |
                    doc_flag::CreateAsDeleted,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocDictUpsert>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::DICT_UPSERT,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            SUBDOC_FLAG_NONE,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add |
                    doc_flag::CreateAsDeleted,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocDelete>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::REMOVE,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ false,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocReplace>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::REPLACE,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted | mcbp::subdoc::doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits
get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushLast>() {
    using namespace mcbp::subdoc;
    return SubdocCmdTraits{CommandScope::SubJSON,
                           Subdoc::Command::ARRAY_APPEND,
                           cb::mcbp::ClientOpcode::Invalid,
                           SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH |
                                   SUBDOC_FLAG_EXPAND_MACROS,
                           SUBDOC_FLAG_NONE,
                           doc_flag::Mkdoc | doc_flag::AccessDeleted |
                                   doc_flag::Add | doc_flag::CreateAsDeleted,
                           /*request_has_value*/ true,
                           /*allow_empty_path*/ true,
                           ResponseValue::None,
                           /*is_mutator*/ true,
                           SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits
get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushFirst>() {
    using namespace mcbp::subdoc;
    return SubdocCmdTraits{CommandScope::SubJSON,
                           Subdoc::Command::ARRAY_PREPEND,
                           cb::mcbp::ClientOpcode::Invalid,
                           SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH |
                                   SUBDOC_FLAG_EXPAND_MACROS,
                           SUBDOC_FLAG_NONE,
                           doc_flag::Mkdoc | doc_flag::AccessDeleted |
                                   doc_flag::Add | doc_flag::CreateAsDeleted,
                           /*request_has_value*/ true,
                           /*allow_empty_path*/ true,
                           ResponseValue::None,
                           /*is_mutator*/ true,
                           SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocArrayInsert>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::ARRAY_INSERT,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH |
                    SUBDOC_FLAG_EXPAND_MACROS,
            SUBDOC_FLAG_NONE,
            doc_flag::AccessDeleted | doc_flag::Add | doc_flag::CreateAsDeleted,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits
get_traits<cb::mcbp::ClientOpcode::SubdocArrayAddUnique>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::ARRAY_ADD_UNIQUE,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_NONE,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add |
                    doc_flag::CreateAsDeleted,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            ResponseValue::None,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocCounter>() {
    using namespace mcbp::subdoc;
    return {CommandScope::SubJSON,
            Subdoc::Command::COUNTER,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_MKDIR_P | SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_NONE,
            doc_flag::Mkdoc | doc_flag::AccessDeleted | doc_flag::Add |
                    doc_flag::CreateAsDeleted,
            /*request_has_value*/ true,
            /*allow_empty_path*/ false,
            ResponseValue::JSON,
            /*is_mutator*/ true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocGetCount>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::GET_COUNT,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ false,
            /*allow_empty_path*/ true,
            ResponseValue::JSON,
            /*is_mutator*/ false,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits
get_traits<cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr>() {
    return {CommandScope::AttributesAndBody,
            Subdoc::Command::GET,
            cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
            SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_XATTR_PATH,
            mcbp::subdoc::doc_flag::None,
            false,
            false,
            ResponseValue::None,
            true,
            SubdocPath::SINGLE};
}

template <>
inline SubdocCmdTraits get_traits<cb::mcbp::ClientOpcode::SubdocMultiLookup>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::INVALID,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE | SUBDOC_FLAG_XATTR_PATH,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::AccessDeleted,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            ResponseValue::Binary,
            /*is_mutator*/ false,
            SubdocPath::MULTI};
}

template <>
inline SubdocCmdTraits
get_traits<cb::mcbp::ClientOpcode::SubdocMultiMutation>() {
    return {CommandScope::SubJSON,
            Subdoc::Command::INVALID,
            cb::mcbp::ClientOpcode::Invalid,
            SUBDOC_FLAG_NONE,
            SUBDOC_FLAG_NONE,
            mcbp::subdoc::doc_flag::Mkdoc | mcbp::subdoc::doc_flag::Add,
            /*request_has_value*/ true,
            /*allow_empty_path*/ true,
            ResponseValue::Binary,
            /*is_mutator*/ true,
            SubdocPath::MULTI};
}

template <cb::mcbp::ClientOpcode CMD>
SubdocMultiCmdTraits get_multi_traits();

template <>
inline SubdocMultiCmdTraits
get_multi_traits<cb::mcbp::ClientOpcode::SubdocMultiLookup>() {
    // Header + 1byte path.
    return {sizeof(protocol_binary_subdoc_multi_lookup_spec) + 1,
            /*is_mutator*/false};
}

template <>
inline SubdocMultiCmdTraits
get_multi_traits<cb::mcbp::ClientOpcode::SubdocMultiMutation>() {
    // Header + 1byte path (not all mutations need a value - e.g. delete).
    return {sizeof(protocol_binary_subdoc_multi_mutation_spec) + 1,
            /*is_mutator*/true};
}
