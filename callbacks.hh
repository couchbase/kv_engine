/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef CALLBACKS_H
#define CALLBACKS_H 1

/**
 * Interface for callbacks from storage APIs.
 */
template <typename RV>
class Callback {
public:

    virtual ~Callback() {}

    /**
     * Method called on callback.
     */
    virtual void callback(RV &value) {
        (void)value;
        throw std::runtime_error("Nobody should call this.");
    }
};

/**
 * Threadsafe callback implementation that just captures the value.
 */
template <typename T>
class RememberingCallback : public Callback<T> {
public:

    /**
     * Construct a remembering callback.
     */
    RememberingCallback() {
        if(pthread_mutex_init(&mutex, NULL) != 0) {
            throw std::runtime_error("Failed to initialize mutex.");
        }
        if(pthread_cond_init(&cond, NULL) != 0) {
            throw std::runtime_error("Failed to initialize condition.");
        }
        fired = false;
    }

    /**
     * Clean up (including lock resources).
     */
    ~RememberingCallback() {
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cond);
    }

    /**
     * The callback implementation -- just store a value.
     */
    void callback(T &value) {
        LockHolder lh(&mutex);
        val = value;
        fired = true;
        if(pthread_cond_broadcast(&cond) != 0) {
            throw std::runtime_error("Failed to broadcast change.");
        }
    }

    /**
     * Wait for a value to be available.
     *
     * This method will return immediately if a value is currently
     * available, otherwise it will wait indefinitely for a value
     * to arrive.
     */
    void waitForValue() {
        LockHolder lh(&mutex);
        if (!fired) {
            if(pthread_cond_wait(&cond, &mutex) != 0) {
                throw std::runtime_error("Failed to wait for condition.");
            }
        }
        assert(fired);
    }

    /**
     * The value that was captured from the callback.
     */
    T    val;
    /**
     * True if the callback has fired.
     */
    bool fired;

private:
    pthread_mutex_t mutex;
    pthread_cond_t  cond;

    DISALLOW_COPY_AND_ASSIGN(RememberingCallback);
};

#endif /* CALLBACKS_H */
