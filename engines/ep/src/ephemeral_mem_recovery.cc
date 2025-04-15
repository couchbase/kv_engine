#include "ephemeral_mem_recovery.h"

#include "checkpoint_remover.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include <executor/executorpool.h>
#include <gsl/gsl-lite.hpp>
#include <phosphor/phosphor.h>
#include <statistics/cbstat_collector.h>

EphemeralMemRecovery::EphemeralMemRecovery(EventuallyPersistentEngine& e,
                                           KVBucket& bucket,
                                           std::chrono::milliseconds sleepTime)
    : EpNotifiableTask(e,
                       TaskId::EphemeralMemRecovery,
                       std::chrono::duration<double>(sleepTime).count(),
                       false),
      bucket(bucket),
      sleepTime(sleepTime) {
}

void EphemeralMemRecovery::wakeupIfRequired() {
    if (recoveryState.load() == RecoveryState::Initial) {
        EpNotifiableTask::wakeup();
    }
}

void EphemeralMemRecovery::updateSleepTime(
        const std::chrono::milliseconds duration) {
    sleepTime = duration;
    ExecutorPool::get()->snooze(
            getId(), std::chrono::duration<double>(getSleepTime()).count());
}

bool EphemeralMemRecovery::runInner(bool manuallyNotified) {
    TRACE_EVENT0("ep-engine/task", "EphemeralMemRecovery");

    switch (recoveryState) {
    case RecoveryState::Initial:
        if (manuallyNotified || shouldStartRecovery()) {
            recoveryState.store(RecoveryState::WaitingForChkRemovers);
            // Wakeup checkpoint removers, pass this class as waiter
            removersToComplete = bucket.getCheckpointRemoverTaskCount();
            Expects(removersToComplete > 0);
            bucket.wakeUpChkRemoversAndGetNotified(shared_from_this(),
                                                   removersToComplete);
            chkRemoversScheduledHook();
        }
        break;
    case RecoveryState::ChkRemoversCompleted:
        Expects(removersToComplete == 0);
        if (shouldContinueRecovery()) {
            // fail_new_data will have ItemPager disabled so this
            // will have no effect
            bucket.wakeUpStrictItemPager();
        }
        recoveryState.store(RecoveryState::Initial);
        break;
    default:
        break;
    }

    return true;
}

void EphemeralMemRecovery::signal() {
    if (recoveryState == RecoveryState::WaitingForChkRemovers) {
        Expects(removersToComplete > 0);
        // Only wakeup once all removers have completed
        if (--removersToComplete == 0) {
            recoveryState.store(RecoveryState::ChkRemoversCompleted);
            wakeup();
        }
    }
}

bool EphemeralMemRecovery::shouldStartRecovery() const {
    // Total memory usage is above the traditional high watermark
    auto& stats = engine->getEpStats();
    return stats.getEstimatedTotalMemoryUsed() > stats.mem_high_wat;
}

bool EphemeralMemRecovery::shouldContinueRecovery() const {
    // Pageable memory usage is above pageable low watermark
    // or total memory usage is above low watermark.
    auto& stats = engine->getEpStats();
    return bucket.getPageableMemCurrent() >
                   bucket.getPageableMemLowWatermark() ||
           stats.getEstimatedTotalMemoryUsed() > stats.mem_low_wat;
}

// Add a new method to expose stats
void EphemeralMemRecovery::addStats(const StatCollector& collector) {
    using namespace cb::stats;
    collector.addStat(Key::ep_ephemeral_mem_recovery_state,
                      toString(recoveryState.load()));
    collector.addStat(Key::ep_ephemeral_mem_recovery_removers_remaining,
                      removersToComplete.load());
}

std::string EphemeralMemRecovery::toString(RecoveryState state) {
    switch (state) {
    case RecoveryState::Initial:
        return "Initial";
    case RecoveryState::WaitingForChkRemovers:
        return "WaitingForChkRemovers";
    case RecoveryState::ChkRemoversCompleted:
        return "ChkRemoversCompleted";
    }

    return "Unknown";
}
