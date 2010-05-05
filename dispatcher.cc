#include "dispatcher.hh"

extern "C" {
    static void* launch_dispatcher_thread(void* arg);
}

static void* launch_dispatcher_thread(void *arg) {
    Dispatcher *dispatcher = (Dispatcher*) arg;
    try {
        dispatcher->run();
    } catch (std::exception& e) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "dispatcher exception caught: %s\n",
                         e.what());
    } catch(...) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Caught a fatal exception in the dispatcher thread\n");
    }
    return NULL;
}

void Dispatcher::start() {
    if(pthread_create(&thread, NULL, launch_dispatcher_thread, this) != 0) {
        throw std::runtime_error("Error initializing dispatcher thread");
    }
}
