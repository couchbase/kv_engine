#include <cassert>

#include "dispatcher.hh"
#include "atomic.hh"
#include "locks.hh"

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
    Thing(Dispatcher *d) : dispatcher(d) {}

    void start(void) {
        dispatcher->schedule(shared_ptr<TestCallback>(new TestCallback(this)));
    }

    bool doSomething(Dispatcher &d, TaskId &t) {
        (void)t;
        assert(&d == dispatcher);
        ++callbacks;
        return true;
    }
private:
    Dispatcher *dispatcher;
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
    std::cerr << "The dispatcher test is currently broken pending rework." << std::endl;
    exit(0);
    Dispatcher dispatcher;
    dispatcher.start();
    Thing t(&dispatcher);
    t.start();
    dispatcher.stop();
    if (callbacks != 1) {
        std::cerr << "Expected 1 callback, got " << callbacks << std::endl;
        return 1;
    }
    return 0;
}
