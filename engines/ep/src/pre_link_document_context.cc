/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "pre_link_document_context.h"

#include "ep_engine.h"
#include "item.h"

#include <memcached/server_document_iface.h>

void PreLinkDocumentContext::preLink(uint64_t cas, uint64_t seqno) {
    // The vbucket_uuid is currently not being used by the pre_link callback
    // neither is the hlcEpoch
    item_info info = item->toItemInfo(0, HlcCasSeqnoUninitialised);
    info.cas = cas;
    info.seqno = seqno;
    engine.getServerApi()->document->pre_link(*cookie, info);
}
