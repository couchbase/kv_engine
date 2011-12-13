/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
provider ep {
    /**
     * A SpinLock object was created
     * @param lock the address of the newly created spinlock object
     */
    probe spinlock__created(const void *lock);

    /**
     * A SpinLock object was destroyed
     * @param lock the address of the spinlock object that was destroyed
     */
    probe spinlock__destroyed(const void *lock);

    /**
     * Fired when a spin lock was successfully acquired.
     *
     * @param lock the address of the lock
     * @param num the number of iterations we had to spin to acquire the lock
     */
    probe spinlock__acquired(const void *lock, uint32_t num);

    /**
     * A SpinLock object was just released
     * @param lock the address of the spinlock object
     */
    probe spinlock__released(const void *lock);

    /**
     * A Mutex object was created
     * @param mutex the address of the newly created mutex object
     */
    probe mutex__created(const void *mutex);

    /**
     * A Mutex object was destroyed
     * @param mutex the address of the mutex object that was destroyed
     */
    probe mutex__destroyed(const void *mutex);

    /**
     * A Mutex object was just acquired
     * @param mutex the address of the mutex object
     */
    probe mutex__acquired(const void *mutex);

    /**
     * A Mutex object was just released
     * @param mutex the address of the mutex object
     */
    probe mutex__released(const void *mutex);
};

#pragma D attributes Unstable/Unstable/Common provider ep provider
#pragma D attributes Private/Private/Common provider ep module
#pragma D attributes Private/Private/Common provider ep function
#pragma D attributes Unstable/Unstable/Common provider ep name
#pragma D attributes Unstable/Unstable/Common provider ep args
