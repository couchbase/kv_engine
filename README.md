# Eventually Persistent Engine
## Threads
Code in ep-engine is executing in a multithreaded environment, two classes of
thread exist.

1. memcached's threads, for servicing a client and calling in via the
[engine API] (https://github.com/couchbase/memcached/blob/master/include/memcached/engine.h)
2. ep-engine's threads, for running tasks such as the document expiry pager
(see subclasses of `GlobalTasks`).

## Synchronisation Primitives

There are three mutual-exclusion primitives available in ep-engine.

1. `Mutex` exclusive lock - [mutex.h](./src/mutex.h)
2. `RWLock` shared, reader/writer lock - [rwlock.h](./src/rwlock.h)
3. `SpinLock` 1-byte exclusive lock - [atomix.h](./src/atomic.h)

A conditional-variable is also available called `SyncObject`
[syncobject.h](./src/syncobject.h). `SyncObject` glues a `Mutex` and
conditional-variable together in one object.

These primitives are managed via RAII wrappers - [locks.h](./src/locks.h).

1. `LockHolder` - for acquiring a `Mutex` or `SyncObject`.
2. `MultiLockHolder` - for acquiring an array of `Mutex` or `SyncObject`.
3. `WriterLockHolder` - for acquiring write access to a `RWLock`.
4. `ReaderLockHolder` - for acquiring read access to a `RWLock`.
5. `SpinLockHolder` - for acquiring a `SpinLock`.

## Mutex
The general style is to create a `LockHolder` when you need to acquire a
`Mutex`, the constructor will acquire and when the `LockHolder` goes out of
scope, the destructor will release the `Mutex`. For certain use-cases the
caller can explicitly lock/unlock a `Mutex` via the `LockHolder` class.

```c++
Mutex mutex;
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
Mutex mutexes[10];
Object objects[10];
void foo() {
    MultiLockHolder lockHolder(&mutexes, 10);
    for (int ii = 0; ii < 10; ii++) {
        objects[ii].doStuff();
    }
    return;
}
```

## RWLock

`RWLock` allows many readers to acquire it and exclusive access for a writer.
`ReadLockHolder` acquires the lock for a reader and `WriteLockHolder` acquires
the lock for a writer. Neither classes enable manual lock/unlock, all
acquisitions and release are performed via the constructor and destructor.

```c++
RWLock rwLock;
Object thing;

void foo1() {
    ReaderLockHolder(&rwLock);
    if (thing.getData()) {
    ...
    }
}

void foo2() {
    WriterLockHolder(&rwLock);
    thing.setData(...);
}
```

## SyncObject

`SyncObject` inherits from `Mutex` and is thus managed via a `LockHolder` or
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

## SpinLock

A `SpinLock` uses a single byte for the lock and our own code to spin until the
lock is acquired. The intention for this lock is for low contention locks.

The RAII pattern is just like for a Mutex.


```c++
SpinLock spinLock;
void example1() {
    SpinLockHolder lockHolder(&spinLock);
    ...
    return;
}
```

## _UNLOCKED convention

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






