#pragma once

#include "ep_task.h"

class EPStats;
class KVBucket;

/**
 * EphemeralMemRecovery task recovers memory from checkpoint memory usage when
 * the traditional HWM is exceeded and memory recovery is triggered.
 * (auto-delete only) - if memory usage is still above the traditional LWM or
 * the pageable LWM (or both), then ItemPager is woken up to attempt further
 * recovery after all checkpoint recovery tasks have completed.
 *
 * CheckpointMemRecoveryTask is utilised to recovery memory from checkpoints.
 * (auto-delete only) ItemPager is utilised to recover memory by item
 * deletion. ItemPager will only be woken up after all checkpoint
 * recovery tasks have completed execution.

 * State transitions: Initial -> WaitingForChkRemovers -> ChkRemoversCompleted
 *
 * Example execution:
 * 1. State: Initial - Traditional HWM is exceeded and the task is woken up
 * 2. State: Initial - CheckpointMemRecoveryTask/s is woken up
 * 3. State: WaitingForChkRemovers - CheckpointMemRecoveryTask/s all complete
 *    which signal this task to wake up. Memory is recovered from checkpoints
 *    but the LWM (either the traditional or pageable or both) is not reached.
 * 4. State: ChkRemoversCompleted - ItemPager is woken up to attempt further
 *    recovery for auto-delete buckets.
 *
 * Definitions:
 *  TO_FREE = (HWM - LWM)   Required amount of memory to be freed
 *  CHK_USAGE               Memory utilised by checkpoints that can be freed
 *
 * 1. TO_FREE <= CHK_USAGE
 *      -> Free as much memory required from checkpoints to reach LWM.
 * 2. TO_FREE > CHK_USAGE
 *      -> Free as much memory from checkpoints
 *      -> (auto-delete only) free remaining memory to reach LWM
 *         by item deletion, utilising the ItemPager
 */
class EphemeralMemRecovery : public EpNotifiableTask, public cb::Waiter {
public:
    enum class RecoveryState {
        Initial,
        WaitingForChkRemovers,
        ChkRemoversCompleted
    };

    /**
     * @param e the engine
     * @param sleepTime number of milliseconds between periodic executions
     */
    EphemeralMemRecovery(EventuallyPersistentEngine& e,
                         KVBucket& bucket,
                         std::chrono::milliseconds sleepTime);

    // Wakeup the task only if it is in the initial state
    // E.g. The task could be sleeping, waiting for checkpoint removers
    // to complete in which case waking it up will do no useful work.
    void wakeupIfRequired();

    bool runInner(bool manuallyNotified) override;

    std::string getDescription() const override {
        return "Ephemeral Memory Recovery";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::milliseconds(250);
    }

    std::chrono::microseconds getSleepTime() const override {
        return sleepTime.load();
    }

    void updateSleepTime(const std::chrono::milliseconds duration);

    // Called from CheckpointMemoryRecovery
    void signal() override;

private:
    // Check memory usage to start/continue recovery
    bool shouldStartRecovery() const;
    bool shouldContinueRecovery() const;

    KVBucket& bucket;

    std::atomic<std::chrono::milliseconds> sleepTime;
    std::atomic<size_t> removersToComplete{0};
    std::atomic<RecoveryState> recoveryState{RecoveryState::Initial};
};
