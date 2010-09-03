#include "config.h"
#include <unistd.h>
#include <cassert>

#include "dispatcher.hh"
#include "atomic.hh"
#include "locks.hh"
#include "priority.hh"

#define EXPECTED_NUM_CALLBACKS 3

Dispatcher dispatcher;
static Atomic<int> callbacks;

class Thing;

class TestCallback : public DispatcherCallback {
public:
    TestCallback(Thing *t) : thing(t) {
    }

    bool callback(Dispatcher &d, TaskId t);

private:
    Thing *thing;
};

class Thing {
public:
    void start(void) {
        dispatcher.schedule(shared_ptr<TestCallback>(new TestCallback(this)),
                            &tid, Priority::BgFetcherPriority);
    }

    bool doSomething(Dispatcher &d, TaskId &t) {
        assert(t == tid);
        assert(&d == &dispatcher);
        return(++callbacks < EXPECTED_NUM_CALLBACKS);
    }
private:
    TaskId tid;
};

bool TestCallback::callback(Dispatcher &d, TaskId t) {
    return thing->doSomething(d, t);
}

static const char* test_get_logger_name(void) {
    return "dispatcher_test";
}

static void test_get_logger_log(EXTENSION_LOG_LEVEL severity,
                                const void* client_cookie,
                                const char *fmt, ...) {
    (void)severity;
    (void)client_cookie;
    (void)fmt;
    // ignore
}

EXTENSION_LOGGER_DESCRIPTOR* getLogger() {
    static EXTENSION_LOGGER_DESCRIPTOR logger;
    logger.get_name = test_get_logger_name;
    logger.log = test_get_logger_log;
    return &logger;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    alarm(5);
    dispatcher.start();
    Thing t;
    t.start();

    // Wait for some callbacks
    while (callbacks < EXPECTED_NUM_CALLBACKS) {
        usleep(1);
    }

    dispatcher.stop();
    if (callbacks != EXPECTED_NUM_CALLBACKS) {
        std::cerr << "Expected 1 callback, got " << callbacks << std::endl;
        return 1;
    }
    return 0;
}
