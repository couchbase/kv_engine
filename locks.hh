/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LOCKS_H
#define LOCKS_H 1

#include <pthread.h>

#include "base-test.hh"

namespace kvtest {

    /**
     * pthread mutex holder (maintains lock while active).
     */
    class LockHolder {
    public:
        /**
         * Acquire the lock in the given mutex.
         */
        LockHolder(pthread_mutex_t* m) : mutex(m), locked(false) {
            lock();
        }

        /**
         * Release the lock.
         */
        ~LockHolder() {
            unlock();
        }

        void lock() {
            if (pthread_mutex_lock(mutex) != 0) {
                throw std::runtime_error("Failed to acquire lock.");
            }
            locked = true;

        }


        void unlock() {
            if (locked) {
                if (pthread_mutex_unlock(mutex) != 0) {
                    throw std::runtime_error("Failed to release lock.");
                }
                locked = false;
            }
        }

    private:
        pthread_mutex_t *mutex;
        bool locked;

        DISALLOW_COPY_AND_ASSIGN(LockHolder);
    };

}

#endif /* LOCKS_H */
