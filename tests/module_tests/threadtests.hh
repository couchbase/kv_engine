#include <pthread.h>

#include <cassert>
#include <iostream>
#include <vector>
#include <algorithm>

#include "locks.hh"

template <typename T>
class Generator {
public:
    virtual ~Generator() {}
    virtual T operator()() = 0;
};

class CountDownLatch {
public:

    CountDownLatch(int n=1) : count(n) {}

    void decr(void) {
        LockHolder lh(so);
        --count;
        so.notify();
    }

    void wait(void) {
        LockHolder lh(so);
        while (count > 0) {
            so.wait();
        }
    }

private:
    int count;
    SyncObject so;

    DISALLOW_COPY_AND_ASSIGN(CountDownLatch);
};

template <typename T> class SyncTestThread;

template <typename T>
static void *launch_sync_test_thread(void *arg) {
    SyncTestThread<T> *stt = static_cast<SyncTestThread<T>*>(arg);
    stt->run();
    return stt;
}

extern "C" {
   typedef void *(*PTHREAD_MAIN)(void *);
}

template <typename T>
class SyncTestThread {
public:

    SyncTestThread(CountDownLatch *s, CountDownLatch *p, Generator<T> *testGen) :
        startingLine(s), pistol(p), gen(testGen) {}

    SyncTestThread(const SyncTestThread &other) :
        startingLine(other.startingLine),
        pistol(other.pistol),
        gen(other.gen) {}

    void start(void) {
        if(pthread_create(&thread, NULL, (PTHREAD_MAIN)( launch_sync_test_thread<T> ), this) != 0) {
            throw std::runtime_error("Error initializing dispatcher thread");
        }
    }

    void run(void) {
        startingLine->decr();
        pistol->wait();
        result = (*gen)();
    }

    void join(void) {
        if (pthread_join(thread, NULL) != 0) {
            throw std::runtime_error("Failed to join.");
        }
    }

    const T getResult(void) const { return result; };

private:
    CountDownLatch *startingLine;
    CountDownLatch *pistol;
    Generator<T>   *gen;

    T         result;
    pthread_t thread;
};

template <typename T>
static void starter(SyncTestThread<T> &t) { t.start(); }

template <typename T>
static void waiter(SyncTestThread<T> &t) { t.join(); }

template <typename T>
std::vector<T> getCompletedThreads(size_t n, Generator<T> *gen) {
    CountDownLatch startingLine(n), pistol(1);

    SyncTestThread<T> proto(&startingLine, &pistol, gen);
    std::vector<SyncTestThread<T> > threads(n, proto);
    assert(threads.size() == n);
    std::for_each(threads.begin(), threads.end(), starter<T>);

    startingLine.wait();
    pistol.decr();

    std::for_each(threads.begin(), threads.end(), waiter<T>);

    std::vector<T> results;
    typename std::vector<SyncTestThread<T> >::iterator it;
    for (it = threads.begin(); it != threads.end(); ++it) {
        results.push_back(it->getResult());
    }

    return results;
}
