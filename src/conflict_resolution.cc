/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "config.h"

#include "conflict_resolution.h"
#include "item.h"
#include "stored-value.h"

bool SeqBasedResolution::resolve(StoredValue *v, const ItemMetaData &meta,
                                 bool deletion) {
    if (!v->isTempNonExistentItem()) {
        if (v->getRevSeqno() > meta.revSeqno) {
            return false;
        } else if (v->getRevSeqno() == meta.revSeqno) {
            if (v->getCas() > meta.cas) {
                return false;
            } else if (v->getCas() == meta.cas) {
                if (deletion || v->getExptime() > meta.exptime) {
                    return false;
                } else if (v->getExptime() == meta.exptime) {
                    if (v->getFlags() >= meta.flags) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}
