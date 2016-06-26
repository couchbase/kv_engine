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

## Tasks

A task is created by creating a sub-class (the `run()` method is the entry point
of the task) of the `GlobalTask` class and it is scheduled onto one of 4 task
queue types. Each task should be declared in `src/tasks.defs.h` using the TASK
macro. Using this macro ensures correct generation of a task-type ID, priority,
task name and ultimately ensures each task gets its own scheduling statistics.

The recipe is simple.

### Add your task's class name with its priority into `src/tasks.defs.h`
 * A lower value priority is 'higher'.
```
TASK(MyNewTask, 1) // MyNewTask has priority 1.
```

### Create your class and set its ID using `MY_TASK_ID`.

```
class MyNewTask : public GlobalTask {
public:
    MyNewTask(EventuallyPersistentEngine* e)
        : GlobalTask(e/*engine/,
                     MY_TASK_ID(MyNewTask),
                     0.0/*snooze*/){}
...
```

### Define pure-virtual methods in `MyNewTask`
* run method

The run method is invoked when the task is executed. The method should return
true if it should be scheduled again. If false is returned, the instance of the
task is never re-scheduled and will deleted once all references to the instance are
gone.

```
bool run() {
   // Task code here
   return schedule again?;
}
```

* Define the `getDescription` method to aid debugging and statistics.
```
std::string getDescription() {
    return "A brief description of what MyNewTask does";
}
```

### Schedule your task to the desired queue.
```
ExTask myNewTask = new MyNewTask(&engine);
myNewTaskId = ExecutorPool::get()->schedule(myNewTask, NONIO_TASK_IDX);
```

The 4 task queue types are:
* Readers -  `READER_TASK_IDX`
 * Tasks that should primarily only read from 'disk'. They generally read from
the vbucket database files, for example background fetch of a non-resident document.
* Writers (they are allowed to read too) `WRITER_TASK_IDX`
 * Tasks that should primarily only write to 'disk'. They generally write to
the vbucket database files, for example when flushing the write queue.
* Auxilliary IO `AUXIO_TASK_IDX`
 * Tasks that read and write 'disk', but not necessarily the vbucket data files.
* Non IO `NONIO_TASK_IDX`
 * Tasks that do not perform 'disk' I/O.

### Utilise `snooze`

The snooze value of the task sets when the task should be executed. The initial snooze
value is set when constructing `GlobalTask`. A value of 0.0 means attempt to execute
the task as soon as scheduled and 5.0 would be 5 seconds from being scheduled
(scheduled meaning when `ExecutorPool::get()->schedule(...)` is called).

The `run()` function can also call `snooze(double snoozeAmount)` to set how long
before the task is rescheduled.

It is **best practice** for most tasks to actually do a sleep forever from their run function:

```
  snooze(INT_MAX);
```

Using `INT_MAX` means sleep forever and tasks should always sleep until they have
real work todo. Tasks **should not periodically poll for work** with a snooze of
n seconds.

### Utilise `wake()`
When a task has work todo, some other function should be waking the task using the wake method.

```
ExecutorPool::get()->wake(myNewTaskId)`
```
