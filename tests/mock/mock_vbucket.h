/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
 * Mock of the VBucket class.  Wraps the real VBucket class
 * and provides access to functions like processSet() and processAdd().
 */
#pragma once

#include "config.h"
#include "ep_vb.h"

class MockEPVBucket : public EPVBucket {
public:
    MockEPVBucket(id_type i,
                  vbucket_state_t newState,
                  EPStats& st,
                  CheckpointConfig& chkConfig,
                  KVShard* kvshard,
                  int64_t lastSeqno,
                  uint64_t lastSnapStart,
                  uint64_t lastSnapEnd,
                  std::unique_ptr<FailoverTable> table,
                  std::shared_ptr<Callback<id_type>> flusherCb,
                  NewSeqnoCallback newSeqnoCb,
                  Configuration& config,
                  item_eviction_policy_t evictionPolicy,
                  vbucket_state_t initState = vbucket_state_dead,
                  uint64_t purgeSeqno = 0,
                  uint64_t maxCas = 0)
        : EPVBucket(i,
                    vbucket_state_active,
                    st,
                    chkConfig,
                    kvshard,
                    lastSeqno,
                    lastSnapStart,
                    lastSnapEnd,
                    nullptr,
                    flusherCb,
                    nullptr,
                    config,
                    evictionPolicy,
                    initState,
                    purgeSeqno,
                    maxCas) {
    }

    MutationStatus public_processSet(Item& itm, const uint64_t cas) {
        auto hbl = ht.getLockedBucket(itm.getKey());
        StoredValue* v =
                ht.unlocked_find(itm.getKey(), hbl.getBucketNum(), true, false);
        return processSet(hbl, v, itm, cas, true, false).first;
    }

    AddStatus public_processAdd(Item& itm) {
        auto hbl = ht.getLockedBucket(itm.getKey());
        StoredValue* v =
                ht.unlocked_find(itm.getKey(), hbl.getBucketNum(), true, false);
        return processAdd(hbl,
                          v,
                          itm,
                          /*maybeKeyExists*/ true,
                          /*isReplication*/ false)
                .first;
    }

    MutationStatus public_processSoftDelete(const DocKey& key,
                                            StoredValue* v,
                                            uint64_t cas) {
        auto hbl = ht.getLockedBucket(key);
        if (!v) {
            v = ht.unlocked_find(key, hbl.getBucketNum(), false, false);
            if (!v) {
                return MutationStatus::NotFound;
            }
        }
        ItemMetaData metadata;
        metadata.revSeqno = v->getRevSeqno() + 1;
        return processSoftDelete(hbl.getHTLock(),
                                 *v,
                                 cas,
                                 metadata,
                                 VBQueueItemCtx(GenerateBySeqno::Yes,
                                                GenerateCas::Yes,
                                                TrackCasDrift::No,
                                                /*isBackfillItem*/ false,
                                                /*preLinkDocCtx*/ nullptr),
                                 /*use_meta*/ false,
                                 /*bySeqno*/ v->getBySeqno())
                .first;
    }

    bool public_deleteStoredValue(const DocKey& key) {
        auto hbl = ht.getLockedBucket(key);
        StoredValue* v = ht.unlocked_find(key,
                                          hbl.getBucketNum(),
                                          /*wantsDeleted*/ true,
                                          /*trackReference*/ false);
        if (!v) {
            return false;
        }
        return deleteStoredValue(hbl, *v);
    }
};
