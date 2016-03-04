# Eventually Persistent Engine
## Threads
Code in ep-engine is executing in a multithreaded environment, two classes of
thread exist.

1. memcached's threads, for servicing a client and calling in via the
[engine API] (https://github.com/couchbase/memcached/blob/master/include/memcached/engine.h)
2. ep-engine's threads, for running tasks such as the document expiry pager
(see subclasses of `GlobalTasks`).

## Synchronisation Primitives

There are two mutual-exclusion primitives available in ep-engine (in
addition to those provided by the C++ standard library):

1. `RWLock` shared, reader/writer lock - [rwlock.h](./src/rwlock.h)
2. `SpinLock` 1-byte exclusive lock - [atomix.h](./src/atomic.h)

A condition-variable is also available called `SyncObject`
[syncobject.h](./src/syncobject.h). `SyncObject` glues a `std::mutex` and
`std::condition_variable` together in one object.

These primitives are managed via RAII wrappers - [locks.h](./src/locks.h).

1. `LockHolder` - for acquiring a `std::mutex` or `SyncObject`.
2. `MultiLockHolder` - for acquiring an array of `std::mutex` or `SyncObject`.
3. `WriterLockHolder` - for acquiring write access to a `RWLock`.
4. `ReaderLockHolder` - for acquiring read access to a `RWLock`.
5. `SpinLockHolder` - for acquiring a `SpinLock`.

### Mutex
The general style is to create a `LockHolder` when you need to acquire a
`std::mutex`, the constructor will acquire and when the `LockHolder` goes out of
scope, the destructor will release the `std::mutex`. For certain use-cases the
caller can explicitly lock/unlock a `std::mutex` via the `LockHolder` class.

```c++
std::mutex mutex;
void example1() {
    LockHolder lockHolder(&mutex);
    ...
    return;
}

void example2() {
    LockHolder lockHolder(&mutex);
    ...
    lockHolder.unlock();
    ...
    lockHolder.lock();
    ...
    return;
}
```

A `MultiLockHolder` allows an array of locks to be conveniently acquired and
released, and similarly to `LockHolder` the caller can choose to manually
lock/unlock at any time (with all locks locked/unlocked via one call).

```c++
std::mutex mutexes[10];
Object objects[10];
void foo() {
    MultiLockHolder lockHolder(&mutexes, 10);
    for (int ii = 0; ii < 10; ii++) {
        objects[ii].doStuff();
    }
    return;
}
```

### RWLock

`RWLock` allows many readers to acquire it and exclusive access for a writer.
`ReadLockHolder` acquires the lock for a reader and `WriteLockHolder` acquires
the lock for a writer. Neither classes enable manual lock/unlock, all
acquisitions and release are performed via the constructor and destructor.

```c++
RWLock rwLock;
Object thing;

void foo1() {
    ReaderLockHolder rlh(&rwLock);
    if (thing.getData()) {
    ...
    }
}

void foo2() {
    WriterLockHolder wlh(&rwLock);
    thing.setData(...);
}
```

### SyncObject

`SyncObject` inherits from `std::mutex` and is thus managed via a `LockHolder` or
`MultiLockHolder`. The `SyncObject` provides the conditional-variable
synchronisation primitive enabling threads to block and be woken.

The wait/wakeOne/wake method is provided by the `SyncObject`.

Note that `wake` will wake up a single blocking thread, `wakeOne` will wake up
every thread that is blocking on the `SyncObject`.

```c++
SyncObject syncObject;
bool sleeping = false;
void foo1() {
    LockHolder lockHolder(&syncObject);
    sleeping = true;
    syncObject.wait(); // the mutex is released and the thread put to sleep
    // when wait returns the mutex is reacquired
    sleeping = false;
}

void foo2() {
    LockHolder lockHolder(&syncObject);
    if (sleeping) {
        syncObject.notifyOne();
    }
}
```

### SpinLock

A `SpinLock` uses a single byte for the lock and our own code to spin until the
lock is acquired. The intention for this lock is for low contention locks.

The RAII pattern is just like for a mutex.


```c++
SpinLock spinLock;
void example1() {
    SpinLockHolder lockHolder(&spinLock);
    ...
    return;
}
```

### _UNLOCKED convention

ep-engine has a function naming convention that indicates the function should
be called with a lock acquired.

For example the following `doStuff_UNLOCKED` method indicates that it expect a
lock to be held before the function is called. What lock should be acquired
before calling is not defined by the convention.

```c++
void Object::doStuff_UNLOCKED() {
}

void Object::run() {
    LockHolder lockHolder(&mutex);
    doStuff_UNLOCKED();
    return;
}
```

## Atomic / thread-safe data structures

In addition to the basic synchronization primitives described above,
there are also the following higher-level data structures which
support atomic / thread-safe access from multiple threads:

1. `AtomicQueue`: thread-safe, approximate-FIFO queue, optimized for
   multiple-writers, one reader - [atomicqueue.h](./src/atomicqueue.h)
2. `AtomicUnorderedMap` : thread-safe unordered map -
   [atomic_unordered_map.h](./src/atomic_unordered_map.h)

## Thread Local Storage (ObjectRegistry).

Threads in ep-engine are servicing buckets and when a thread is dispatched to
serve a bucket, the pointer to the `EventuallyPersistentEngine` representing
the bucket is placed into thread local storage, this avoids the need for the
pointer to be passed along the chain of execution as a formal parameter.

Both threads servicing frontend operations (memcached's threads) and ep-engine's
own task threads will save the bucket's engine pointer before calling down into
engine code.

Calling `ObjectRegistry::onSwitchThread(enginePtr)` will save the `enginePtr`
in thread-local-storage so that subsequent task code can retrieve the pointer
with `ObjectRegistry::getCurrentEngine()`.






