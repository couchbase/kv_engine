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
    case cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr:
        return get_traits<cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr>();

        // The following ops does not support SubDoc
    default:
        return {CommandScope::SubJSON,
                Subdoc::Command::INVALID,
                cb::mcbp::ClientOpcode::Invalid,
                SUBDOC_FLAG_NONE,
                SUBDOC_FLAG_NONE,
                cb::mcbp::subdoc::doc_flag::None,
                false,
                false,
                ResponseValue::None,
                false,
                SubdocPath::SINGLE};
    }
}
