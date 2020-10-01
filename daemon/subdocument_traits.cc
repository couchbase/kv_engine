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

#include "subdocument_traits.h"

cb::mcbp::Datatype SubdocCmdTraits::responseDatatype(
        protocol_binary_datatype_t docDatatype) const {
    switch (response_type) {
    case ResponseValue::None:
        // datatype is mandatory for responses; return RAW even in the
        // no-response-value case.
        return cb::mcbp::Datatype::Raw;
    case ResponseValue::JSON:
        return cb::mcbp::Datatype::JSON;
    case ResponseValue::Binary:
        return cb::mcbp::Datatype::Raw;
    case ResponseValue::FromDocument:
        return cb::mcbp::Datatype(docDatatype);
    }
    throw std::logic_error("responseDatatype: invalid response_type:" +
                           std::to_string(int(response_type)));
}

SubdocCmdTraits get_subdoc_cmd_traits(cb::mcbp::ClientOpcode cmd) {
    switch (cmd) {
    case cb::mcbp::ClientOpcode::Get:
        return get_traits<cb::mcbp::ClientOpcode::Get>();

    case cb::mcbp::ClientOpcode::Set:
        return get_traits<cb::mcbp::ClientOpcode::Set>();

    case cb::mcbp::ClientOpcode::Delete:
        return get_traits<cb::mcbp::ClientOpcode::Delete>();

    case cb::mcbp::ClientOpcode::SubdocGet:
        return get_traits<cb::mcbp::ClientOpcode::SubdocGet>();

    case cb::mcbp::ClientOpcode::SubdocExists:
        return get_traits<cb::mcbp::ClientOpcode::SubdocExists>();

    case cb::mcbp::ClientOpcode::SubdocDictAdd:
        return get_traits<cb::mcbp::ClientOpcode::SubdocDictAdd>();

    case cb::mcbp::ClientOpcode::SubdocDictUpsert:
        return get_traits<cb::mcbp::ClientOpcode::SubdocDictUpsert>();

    case cb::mcbp::ClientOpcode::SubdocDelete:
        return get_traits<cb::mcbp::ClientOpcode::SubdocDelete>();

    case cb::mcbp::ClientOpcode::SubdocReplace:
        return get_traits<cb::mcbp::ClientOpcode::SubdocReplace>();

    case cb::mcbp::ClientOpcode::SubdocArrayPushLast:
        return get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushLast>();

    case cb::mcbp::ClientOpcode::SubdocArrayPushFirst:
        return get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushFirst>();

    case cb::mcbp::ClientOpcode::SubdocArrayInsert:
        return get_traits<cb::mcbp::ClientOpcode::SubdocArrayInsert>();

    case cb::mcbp::ClientOpcode::SubdocArrayAddUnique:
        return get_traits<cb::mcbp::ClientOpcode::SubdocArrayAddUnique>();

    case cb::mcbp::ClientOpcode::SubdocCounter:
        return get_traits<cb::mcbp::ClientOpcode::SubdocCounter>();

    case cb::mcbp::ClientOpcode::SubdocGetCount:
        return get_traits<cb::mcbp::ClientOpcode::SubdocGetCount>();

    default:
        return {CommandScope::SubJSON,
                Subdoc::Command::INVALID,
                cb::mcbp::ClientOpcode::Invalid,
                SUBDOC_FLAG_NONE,
                SUBDOC_FLAG_NONE,
                mcbp::subdoc::doc_flag::None,
                false,
                false,
                ResponseValue::None,
                false,
                SubdocPath::SINGLE};
    }
}
