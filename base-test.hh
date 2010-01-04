#ifndef BASE_TEST_H
#define BASE_TEST_H 1

#include <assert.h>
#include <stdbool.h>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <list>

// Stolen from http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml
// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#include "locks.hh"
#include "callbacks.hh"

/*! \mainpage kvtest
 *
 * \section intro_sec Introduction
 *
 * kvtest is a tool for benchmarking simple low-level data stores and
 * experimenting with strategies to get better results from them.
 *
 * \example example-test.cc
 * \example sqlite3-test.cc
 * \example sqlite3-async-test.cc
 * \example bdb-test.cc
 * \example bdb-async-test.cc
 */

/**
 * General kvtest interfaces.
 */
namespace kvtest {

    /**
     * Value for callback for GET operations.
     */
    class GetValue {
    public:
        GetValue() { }

        GetValue(std::string v, bool s) {
            value = v;
            success = s;
        }

        friend std::ostream& operator<<(std::ostream &o, GetValue &gv) {
            return o << "{GetValue success=" << gv.success
                     << ", value=\"" << gv.value << "\"}";
        }

        /**
         * The value retrieved for the key.
         */
        std::string value;
        /**
         * True if a value was successfully retrieved.
         */
        bool success;
    };

    /**
     * An individual kv storage (or way to access a kv storage).
     */
    class KVStore {
    public:

        KVStore() {}

        virtual ~KVStore() {}

        /**
         * Called after each test to reinitialize the test.
         */
        virtual void reset() {}

        /**
         * Method that should not return until the driver has done its job.
         *
         * @param c the callback that will fire when the noop is evalutated
         */
        virtual void noop(Callback<bool> &c) {
            bool t = true;
            c.callback(t);
        }

        /**
         * Set a given key and value.
         *
         * @param key the key to set
         * @param val the value to set
         * @param cb callback that will fire with true if the set succeeded
         */
        virtual void set(std::string &key, std::string &val,
                         Callback<bool> &cb) = 0;

        /**
         * Set a given key and (character) value.
         *
         * @param key the key to set
         * @param val the value to set
         * @param val the number of bytes in the value
         * @param cb callback that will fire with true if the set succeeded
         */
        virtual void set(std::string &key, const char *val, size_t nbytes,
                         Callback<bool> &cb) = 0;

        /**
         * Get the value for the given key.
         *
         * @param key the key
         * @param cb callback that will fire with the retrieved value
         */
        virtual void get(std::string &key, Callback<GetValue> &cb) = 0;

        /**
         * Delete a value for a key.
         *
         * @param key the key
         * @param cb callback that will fire with true if the value
         *           existed and then was deleted
         */
        virtual void del(std::string &key, Callback<bool> &cb) = 0;

        /**
         * For things that support transactions, this signals the
         * beginning of one.
         */
        virtual void begin() {}

        /**
         * For things that support transactions, this signals the
         * successful completion of one.
         */
        virtual void commit() {}

        /**
         * For things that support transactions, this signals the
         * unsuccessful completion of one.
         */
        virtual void rollback() {}

    private:
        DISALLOW_COPY_AND_ASSIGN(KVStore);
    };

    /**
     * Assertion errors.
     */
    class AssertionError : public std::runtime_error {
    public:
        AssertionError(const char *s) : std::runtime_error(s) { }
        AssertionError(const std::string s) : std::runtime_error(s) { }
    };

    /**
     * Assertion mixins.
     */
    class Assertions {
    public:
        inline void assertTrue(bool v, std::string msg) {
            if(!v) {
                throw AssertionError(msg);
            }
        }

        inline void assertFalse(bool v, std::string msg) {
            assertTrue(!v, msg);
        }

        inline void assertEquals(std::string s1, std::string s2) {
            assertTrue(s1.compare(s2) == 0, "failed string compare");
        }

        inline void assertEquals(int i1, int i2) {
            std::stringstream ss;
            ss << "Expected " << i1 << " got " << i2;
            assertTrue(i1 == i2, ss.str());
        }

        inline void assertNull(std::string *s) {
            if (s != NULL) {
                std::string msg = "expected null, got ``" + *s + "''";
                throw AssertionError(msg);
            }
        }

        inline void assertNotNull(std::string *s) {
            assertTrue(s != NULL, "expected nonnull");
        }
    };
}

#endif /* BASE_TEST_H */
