/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "pre_link_document_context.h"
#include <memcached/types.h>
#include "ep_engine.h"

void PreLinkDocumentContext::setCas(uint64_t cas) {
    // The vbucket_uuid is currently not being used by the pre_link callback
    item_info info = item->toItemInfo(0);
    info.cas = cas;
    engine.getServerApi()->document->pre_link(cookie, info);
}
