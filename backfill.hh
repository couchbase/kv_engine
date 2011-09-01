/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef BACKFILL_HH
#define BACKFILL_HH 1

#include <assert.h>
#include <set>

#include "common.hh"
#include "stats.hh"
#include "dispatcher.hh"
#include "ep_engine.h"

#define DEFAULT_BACKFILL_RESIDENT_THRESHOLD 0.9
#define MINIMUM_BACKFILL_RESIDENT_THRESHOLD 0.7
#define BACKFILL_MEM_THRESHOLD 0.9

/**
 * Dispatcher callback responsible for bulk backfilling tap queues
 * from a KVStore.
 *
 * Note that this is only used if the KVStore reports that it has
 * efficient vbucket ops.
 */
class BackfillDiskLoad : public DispatcherCallback, public Callback<GetValue> {
public:

    BackfillDiskLoad(const std::string &n, EventuallyPersistentEngine* e,
                     TapConnMap &tcm, KVStore *s, uint16_t vbid, const void *token)
        : name(n), engine(e), connMap(tcm), store(s), vbucket(vbid), validityToken(token) {

        vbucket_version = engine->getEpStore()->getVBucketVersion(vbucket);
    }

    void callback(GetValue &gv);

    bool callback(Dispatcher &, TaskId);

    std::string description();

private:
    const std::string           name;
    EventuallyPersistentEngine *engine;
    TapConnMap                 &connMap;
    KVStore                    *store;
    uint16_t                    vbucket;
    uint16_t                    vbucket_version;
    const void                 *validityToken;
};

/**
 * VBucketVisitor to backfill a TapProducer. This visitor basically performs backfill from memory
 * for only resident items if it needs to schedule a separate disk backfill task because of
 * low resident ratio.
 */
class BackFillVisitor : public VBucketVisitor {
public:
    BackFillVisitor(EventuallyPersistentEngine *e, TapProducer *tc,
                    const void *token, const VBucketFilter &backfillVBfilter):
        VBucketVisitor(backfillVBfilter), engine(e), name(tc->getName()),
        queue(new std::list<queued_item>),
        found(), validityToken(token),
        maxBackfillSize(e->tapBacklogLimit), valid(true),
        efficientVBDump(e->epstore->getStorageProperties().hasEfficientVBDump()),
        residentRatioBelowThreshold(false) {
        found.reserve(e->tapBacklogLimit);
    }

    virtual ~BackFillVisitor() {
        delete queue;
    }

    void releaseEngineResources() {
        engine->tapConnMap.releaseValidityToken(validityToken);
    }

    bool visitBucket(RCPtr<VBucket> vb);

    void visit(StoredValue *v);

    bool shouldContinue() {
        return checkValidity();
    }

    void apply(void);

    void complete(void);

    static void setResidentItemThreshold(double residentThreshold);

private:

    void setEvents();

    bool pauseVisitor();

    bool checkValidity();

    EventuallyPersistentEngine *engine;
    const std::string name;
    std::list<queued_item> *queue;
    std::vector<std::pair<uint16_t, queued_item> > found;
    std::vector<uint16_t> vbuckets;
    const void *validityToken;
    ssize_t maxBackfillSize;
    bool valid;
    bool efficientVBDump;
    bool residentRatioBelowThreshold;

    static double backfillResidentThreshold;
};

/**
 * Backfill task scheduled by non-IO dispatcher. Each backfill task performs backfill from
 * memory or disk depending on the resident ratio. Each backfill task can backfill more than one
 * vbucket, but will snooze for 1 sec if the current backfill backlog for the corresponding TAP
 * producer is greater than the threshold (5000 by default).
 */
class BackfillTask : public DispatcherCallback {
public:

    BackfillTask(EventuallyPersistentEngine *e, TapProducer *tc,
                 EventuallyPersistentStore *s, const void *tok,
                 const VBucketFilter &backfillVBFilter):
      bfv(new BackFillVisitor(e, tc, tok, backfillVBFilter)), engine(e), epstore(s) {}

    virtual ~BackfillTask() {}

    bool callback(Dispatcher &d, TaskId t);

    std::string description() {
        return std::string("Backfilling items from memory and disk.");
    }

    shared_ptr<BackFillVisitor> bfv;
    EventuallyPersistentEngine *engine;
    EventuallyPersistentStore *epstore;
};

#endif /* BACKFILL_HH */
