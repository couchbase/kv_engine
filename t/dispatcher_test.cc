#include "config.h"
#include <unistd.h>
#include <cassert>

#include "dispatcher.hh"
#include "atomic.hh"
#include "locks.hh"
#include "priority.hh"
#include "ep_engine.h"

EventuallyPersistentEngine *engine = NULL;
Dispatcher dispatcher(*engine);
static Atomic<int> callbacks;

extern "C" {
    static rel_time_t basic_current_time(void) {
        return 0;
    }

    rel_time_t (*ep_current_time)() = basic_current_time;
}

class Thing;

class TestCallback : public DispatcherCallback {
public:
    TestCallback(Thing *t) : thing(t) {
    }

    bool callback(Dispatcher &d, TaskId t);

    std::string description() { return std::string("Test"); }

private:
    Thing *thing;
};

class Thing {
public:
    void start(double sleeptime=0) {
        dispatcher.schedule(shared_ptr<TestCallback>(new TestCallback(this)),
                            NULL, Priority::BgFetcherPriority, sleeptime);
        dispatcher.schedule(shared_ptr<TestCallback>(new TestCallback(this)),
                            NULL, Priority::FlusherPriority, sleeptime);
        dispatcher.schedule(shared_ptr<TestCallback>(new TestCallback(this)),
                            NULL, Priority::VBucketDeletionPriority, 0, false);
    }

    bool doSomething(Dispatcher &d, TaskId &t) {
        (void)d; (void)t;
        ++callbacks;
        return false;
    }
};

bool TestCallback::callback(Dispatcher &d, TaskId t) {
    return thing->doSomething(d, t);
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    int expected_num_callbacks=3;
    Thing t;

    alarm(5);
    dispatcher.start();
    t.start();
    // Wait for some callbacks
    while (callbacks < expected_num_callbacks) {
        usleep(1);
    }
    if (callbacks != expected_num_callbacks) {
        std::cerr << "Expected " << expected_num_callbacks << " callbacks, but got "
                  << callbacks << std::endl;
        return 1;
    }

    callbacks=0;
    expected_num_callbacks=1;
    t.start(3);
    dispatcher.stop();
    if (callbacks != expected_num_callbacks) {
        std::cerr << "Expected " << expected_num_callbacks << " callbacks, but got "
                  << callbacks << std::endl;
        return 1;
    }

    IdleTask it;
    assert(hrtime2text(it.maxExpectedDuration()) == std::string("3600 s"));

    return 0;
}
