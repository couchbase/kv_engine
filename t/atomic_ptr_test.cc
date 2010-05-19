#include <assert.h>
#include <pthread.h>

#include "atomic.hh"
#include "locks.hh"
#define NUM_THREADS 50
#define NUM_TIMES 1000000

class Doodad : public RCValue {
public:
    Doodad() {
        numInstances++;
    }

    ~Doodad() {
        numInstances--;
    }

    static int getNumInstances() {
        return numInstances;
    }

private:
    static Atomic<int> numInstances;
};

Atomic<int> Doodad::numInstances(0);

struct thread_args {
    SyncObject mutex;
    SyncObject gate;
    int counter;
    RCPtr<Doodad> ptr;
};

void *launch_test_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    int i(0);
    LockHolder lh(args->mutex);
    args->gate.acquire();
    args->counter++;
    args->gate.release();
    args->gate.notify();
    args->mutex.wait();
    lh.unlock();
    for (i = 0; i < NUM_TIMES; ++i) {
        switch (rand() % 4) {
        case 0:
            args->ptr.reset(new Doodad);
            break;
        case 1:
            {
                RCPtr<Doodad> d(new Doodad);
                args->ptr.reset(d);
            }
            break;
        case 2:
            {
                RCPtr<Doodad> d(new Doodad);
                d.reset();
            }
            break;
        case 3:
            {
                RCPtr<Doodad> d(args->ptr);
                d.reset();
            }
            break;
        default:
            assert(false);
        }
    }

    return static_cast<void *>(0);
}

int main() {
    pthread_t threads[NUM_THREADS];
    int i(0), rc(0), result(-1);
    struct thread_args args;

    args.counter = 0;

    for (i = 0; i < NUM_THREADS; ++i) {
        rc = pthread_create(&threads[i], NULL, launch_test_thread, &args);
        assert(rc == 0);
    }

    // Wait for all threads to reach the starting gate
    int counter;
    while (true) {
        LockHolder lh(args.gate);
        counter = args.counter;
        if (counter == NUM_THREADS) {
            break;
        }
        args.gate.wait();
    }
    args.mutex.notify();

    for (i = 0; i < NUM_THREADS; ++i) {
        rc = pthread_join(threads[i], reinterpret_cast<void **>(&result));
        assert(rc == 0);
        assert(result == 0);
    }
    args.ptr.reset();
    assert(Doodad::getNumInstances() == 0);
    return 0;
}
