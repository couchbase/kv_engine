/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_FLUSHER_H_
#define SRC_FLUSHER_H_ 1

#include "common.h"
#include "ep.h"
#include "dispatcher.h"
#include "mutation_log.h"

enum flusher_state {
    initializing,
    running,
    pausing,
    paused,
    stopping,
    stopped
};

class Flusher;

const double DEFAULT_MIN_SLEEP_TIME = 0.1;
const size_t MIN_CHK_FLUSH_TIMEOUT = 10; // 10 sec.
const size_t MAX_CHK_FLUSH_TIMEOUT = 30; // 30 sec.

/**
 * A DispatcherCallback adaptor over Flusher.
 */
class FlusherStepper : public DispatcherCallback {
public:
    FlusherStepper(Flusher* f) : flusher(f) { }
    bool callback(Dispatcher &d, TaskId &t);

    std::string description() {
        return std::string("Running a flusher loop.");
    }

    hrtime_t maxExpectedDuration() {
        // Flusher can take a while, but let's report if it runs for
        // more than ten minutes.
        return 10 * 60 * 1000 * 1000;
    }

private:
    Flusher *flusher;
};

struct HighPriorityVBEntry {
    HighPriorityVBEntry() :
        cookie(NULL), checkpoint(0), start(gethrtime()) { }
    HighPriorityVBEntry(const void *c, uint64_t chk) :
        cookie(c), checkpoint(chk), start(gethrtime()) { }

    const void *cookie;
    uint64_t checkpoint;
    hrtime_t start;
};

/**
 * Manage persistence of data for an EventuallyPersistentStore.
 */
class Flusher {
public:

    Flusher(EventuallyPersistentStore *st, Dispatcher *d) :
        store(st), _state(initializing), dispatcher(d),
        flushRv(0), prevFlushRv(0), minSleepTime(0.1),
        flushQueue(NULL),
        forceShutdownReceived(false), flushPhase(0), nextVbid(0),
        chkFlushTimeout(MIN_CHK_FLUSH_TIMEOUT) { }

    ~Flusher() {
        if (_state != stopped) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Flusher being destroyed in state %s\n",
                             stateName(_state));

        }
    }

    bool stop(bool isForceShutdown = false);
    void wait();
    bool pause();
    bool resume();

    void initialize(TaskId &);

    void start(void);
    void wake(void);
    bool step(Dispatcher&, TaskId &);

    enum flusher_state state() const;
    const char * stateName() const;

    void addHighPriorityVBEntry(uint16_t vbid, uint64_t chkid,
                                const void *cookie);
    void removeHighPriorityVBEntry(uint16_t vbid, const void *cookie);
    void getAllHighPriorityVBuckets(std::vector<uint16_t> &vbs);
    std::list<HighPriorityVBEntry> getHighPriorityVBEntries(uint16_t vbid);
    size_t getNumOfHighPriorityVBs() const;

    size_t getCheckpointFlushTimeout() const;
    void adjustCheckpointFlushTimeout(size_t wall_time);

private:
    bool transition_state(enum flusher_state to);
    int doFlush();
    void completeFlush();
    void schedule_UNLOCKED();
    double computeMinSleepTime();

    const char * stateName(enum flusher_state st) const;

    EventuallyPersistentStore   *store;
    volatile enum flusher_state  _state;
    Mutex                        taskMutex;
    TaskId                       task;
    Dispatcher                  *dispatcher;

    // Current flush cycle state.
    int                      flushRv;
    int                      prevFlushRv;
    double                   minSleepTime;
    vb_flush_queue_t        *flushQueue;
    rel_time_t               flushStart;

    Atomic<bool> forceShutdownReceived;

    Mutex priorityVBMutex;
    std::map<uint16_t, std::list<HighPriorityVBEntry> > priorityVBList;
    size_t flushPhase;
    uint16_t nextVbid;
    size_t chkFlushTimeout;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};

#endif  // SRC_FLUSHER_H_
