/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cstdint>
#include <string>

namespace cb::mcbp {

/**
 * Definition of hello's features.
 * Note regarding JSON:0x1. Previously this was named DATATYPE and
 * implied that when supported all bits of the protocol datatype byte would
 * be valid. DATATYPE was never enabled and has been renamed as
 * JSON. Clients are now required negotiate individual datatypes
 * with the server using the feature DataType_* feature codes. Note XATTR
 * is linked with general xattr support and the ability to set the xattr
 * datatype bit using set_with_meta.
 */
enum class Feature : uint16_t {
    Invalid = 0x01, // Previously DATATYPE, now retired
    TLS = 0x2,
    TCPNODELAY = 0x03,
    MUTATION_SEQNO = 0x04,
    TCPDELAY = 0x05,
    XATTR = 0x06, // enables xattr support and set_with_meta.datatype == xattr
    XERROR = 0x07,
    SELECT_BUCKET = 0x08,
    Invalid2 = 0x09, // Used to be collections
    SNAPPY = 0x0a,
    JSON = 0x0b,
    Duplex = 0x0c,
    /**
     * Request the server to push any cluster maps stored by ns_server into
     * one of the buckets the client have access to.
     */
    ClustermapChangeNotification = 0x0d,
    /**
     * Tell the server that we're ok with the server reordering the execution
     * of commands (@todo this should "disable" select bucket as that won't
     * give the user deterministic behavior)
     */
    UnorderedExecution = 0x0e,
    /**
     * Tell the server to enable tracing of function calls
     */
    Tracing = 0x0f,
    /// Does the server support alternative request packets
    AltRequestSupport = 0x10,
    /// Do the server support Synchronous Replication
    SyncReplication = 0x11,

    Collections = 0x12,

    /// Do the server support OpenTelemetry
    OpenTracing = 0x13,

    /// Do the server support preserving document expiry time
    PreserveTtl = 0x14,

    /// Does the server support the $vbucket in addition to the original
    /// $document and $XTOC VATTRs?
    /// Additionally, is non-existence of a VATTR flagged with
    /// SubdocXattrUnknownVattr instead of disconnecting the client?
    VAttr = 0x15,

    // Does the server support Point in Time Recovery
    PiTR = 0x16,

    /// Does the server support the subdoc mutation flag
    /// mcbp::subdoc::doc_flag::CreateAsDeleted ?
    SubdocCreateAsDeleted = 0x17,

    /// Does the server support using the virtual $document attributes in macro
    /// expansion ( "${document.CAS}" etc)
    SubdocDocumentMacroSupport = 0x18,

    /// Does the server support SubdocReplaceBodyWithXattr introduced in
    /// Cheshire-Cat
    SubdocReplaceBodyWithXattr = 0x19,
};

} // namespace cb::mcbp

std::string to_string(cb::mcbp::Feature feature);
