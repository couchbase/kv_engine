#ifndef KEYS_HH
#define KEYS_HH 1

#include <vector>

#include <iostream>

#include "base-test.hh"

namespace kvtest {

    /**
     * A collection of keys to be reused for a test.
     */
    class Keys {
    public:
        /**
         * Instantiate a Keys object that generates the given number of keys.
         */
        Keys(size_t numKeys);

        /**
         * Clean up the allocated keys.
         */
        ~Keys();

        /**
         * Grab a key.
         */
        const char* nextKey();

        /**
         * Get the number of keys in this Keys object.
         */
        size_t length();
    private:
        std::vector<const char *> keys;
        std::vector<const char *>::iterator it;
    };

}

#endif /* KEYS_HH */
