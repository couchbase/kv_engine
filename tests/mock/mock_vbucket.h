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
        int bucketNum(0);
        auto lh = ht.getLockedBucket(itm.getKey(), &bucketNum);
        StoredValue* v =
                ht.unlocked_find(itm.getKey(), bucketNum, true, false);
        return processSet(lh, v, itm, cas, true, false, bucketNum).first;
    }

    AddStatus public_processAdd(Item& itm) {
        int bucket_num(0);
        auto lh = ht.getLockedBucket(itm.getKey(), &bucket_num);
        StoredValue* v =
                ht.unlocked_find(itm.getKey(), bucket_num, true, false);
        return processAdd(lh,
                          v,
                          itm,
                          /*maybeKeyExists*/ true,
                          /*isReplication*/ false,
                          bucket_num)
                .first;
    }

    MutationStatus public_processSoftDelete(const DocKey& key,
                                            StoredValue* v,
                                            uint64_t cas) {
        int bucket_num(0);
        auto lh = ht.getLockedBucket(key, &bucket_num);
        if (!v) {
            v = ht.unlocked_find(key, bucket_num, false, false);
            if (!v) {
                return MutationStatus::NotFound;
            }
        }
        ItemMetaData metadata;
        metadata.revSeqno = v->getRevSeqno() + 1;
        return processSoftDelete(lh,
                                 *v,
                                 cas,
                                 metadata,
                                 VBQueueItemCtx(GenerateBySeqno::Yes,
                                                GenerateCas::Yes,
                                                TrackCasDrift::No,
                                                /*isBackfillItem*/ false),
                                 /*use_meta*/ false,
                                 /*bySeqno*/ v->getBySeqno())
                .first;
    }

    bool public_deleteStoredValue(const DocKey& key) {
        int bucket_num(0);
        std::unique_lock<std::mutex> lh = ht.getLockedBucket(key, &bucket_num);
        StoredValue* v = ht.unlocked_find(key,
                                          bucket_num,
                                          /*wantsDeleted*/ true,
                                          /*trackReference*/ false);
        if (!v) {
            return false;
        }
        return deleteStoredValue(lh, *v, bucket_num);
    }
};
