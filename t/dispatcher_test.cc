#include <cassert>

#include "dispatcher.hh"
#include "atomic.hh"
#include "locks.hh"

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
        LockHolder lh(m);
        TaskId t = dispatcher.schedule(shared_ptr<TestCallback>(new TestCallback(this)));
        sleep(1); // simulate an artificial delay allowing another thread in
        tid = t;
    }

    bool doSomething(Dispatcher &d, TaskId &t) {
        LockHolder lh(m);
        assert(t == tid);
        assert(&d == &dispatcher);
        ++callbacks;
        return true;
    }
private:
    TaskId tid;
    Mutex m;
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
    dispatcher.start();
    Thing t;
    t.start();
    dispatcher.stop();
    assert(callbacks == 1);
}
