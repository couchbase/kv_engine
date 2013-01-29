/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "mutation_log_compactor.hh"
#include "ep.hh"

/**
 * Visit all the items in memory and dump them into a new mutation log file.
 */
class LogCompactionVisitor : public VBucketVisitor {
public:
    LogCompactionVisitor(MutationLog &log, EPStats &st)
        : mutationLog(log), stats(st), numItemsLogged(0), totalItemsLogged(0)
    { /* EMPTY */ }

    void visit(StoredValue *v) {
        if (!v->isDeleted() && v->hasId()) {
            ++numItemsLogged;
            mutationLog.newItem(currentBucket->getId(), v->getKey(), v->getId());
        }
    }

    bool visitBucket(RCPtr<VBucket> &vb) {
        update();
        return VBucketVisitor::visitBucket(vb);
    }

    void update() {
        if (numItemsLogged > 0) {
            mutationLog.commit1();
            mutationLog.commit2();
            LOG(EXTENSION_LOG_INFO,
                "Mutation log compactor: Dumped %ld items from VBucket %d "
                "into a new mutation log file.",
                numItemsLogged, currentBucket->getId());
            totalItemsLogged += numItemsLogged;
            numItemsLogged = 0;
        }
    }

    void complete() {
        update();
        LOG(EXTENSION_LOG_INFO,
            "Mutation log compactor: Completed by dumping total %ld items "
            "into a new mutation log file.", totalItemsLogged);
    }

private:
    MutationLog &mutationLog;
    EPStats     &stats;
    size_t       numItemsLogged;
    size_t       totalItemsLogged;
};

bool MutationLogCompactor::callback(Dispatcher &d, TaskId &t) {
    size_t num_new_items = mutationLog.itemsLogged[ML_NEW];
    size_t num_del_items = mutationLog.itemsLogged[ML_DEL];
    size_t num_logged_items = num_new_items + num_del_items;
    size_t num_unique_items = num_new_items - num_del_items;
    size_t queue_size = stats.diskQueueSize.get();

    bool rv = true;
    bool schedule_compactor =
        mutationLog.logSize > compactorConfig.getMaxLogSize() &&
        num_logged_items > (num_unique_items * compactorConfig.getMaxEntryRatio()) &&
        queue_size < compactorConfig.getQueueCap();

    if (schedule_compactor) {
        std::string compact_file = mutationLog.getLogFile() + ".compact";
        if (access(compact_file.c_str(), F_OK) == 0 &&
            remove(compact_file.c_str()) != 0) {
            LOG(EXTENSION_LOG_WARNING,
                "Can't remove the existing compacted log file \"%s\"",
                compact_file.c_str());
            return false;
        }

        BlockTimer timer(&stats.mlogCompactorHisto, "klogCompactorTime", stats.timingLog);
        epStore->pauseFlusher();
        try {
            MutationLog new_log(compact_file, mutationLog.getBlockSize());
            new_log.open();
            assert(new_log.isEnabled());
            new_log.setSyncConfig(mutationLog.getSyncConfig());

            LogCompactionVisitor compact_visitor(new_log, stats);
            epStore->visit(compact_visitor);
            mutationLog.replaceWith(new_log);
        } catch (MutationLog::ReadException e) {
            LOG(EXTENSION_LOG_WARNING,
                "Error in creating a new mutation log for compaction:  %s",
                e.what());
        } catch (...) {
            LOG(EXTENSION_LOG_WARNING, "Fatal error caught in task \"%s\"",
                description().c_str());
        }

        if (!mutationLog.isOpen()) {
            mutationLog.disable();
            rv = false;
        }
        epStore->resumeFlusher();
        ++stats.mlogCompactorRuns;
    }

    d.snooze(t, compactorConfig.getSleepTime());
    return rv;
}
