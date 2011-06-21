/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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
#ifndef CONFIGURATION_HH
#define CONFIGURATION_HH

#include <string>
#include <map>
#include <assert.h>
#include <iostream>

#include "locks.hh"
#include "memcached/engine.h"

/**
 * The value changed listeners runs _without_ the global mutex for
 * the configuration class, so you may access other configuration
 * members from the callback.
 * The callback is fired <b>after</b> the value is set, so if you
 * want to prevent the caller from setting specific values you should
 * use the ValueChangedValidator instead.
 */
class ValueChangedListener {
public:
    /**
     * Callback if when a boolean configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void valueChanged(const std::string &key, bool value) = 0;

    /**
     * Callback if when a numeric configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void valueChanged(const std::string &key, size_t value) = 0;

    /**
     * Callback if when a floatingpoint configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void valueChanged(const std::string &key, float value) = 0;

    /**
     * Callback if when a string configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void valueChanged(const std::string &key, const char *value) = 0;

    virtual ~ValueChangedListener() { /* EMPTY */}
};

/**
 * The validator for the values runs with the mutex held
 * for the configuration class, so you can't try to access
 * any other configuration variables from the callback
 */
class ValueChangedValidator {
public:
    /**
     * Validator for boolean values
     * @param key the key that is about to change
     * @param value the requested new value
     * @return true the value is ok, false the value is not ok
     */
    virtual bool validate(const std::string &key, bool value) = 0;

    /**
     * Validator for a numeric value
     * @param key the key that is about to change
     * @param value the requested new value
     * @return true the value is ok, false the value is not ok
     */
    virtual bool validate(const std::string &key, size_t value) = 0;

    /**
     * Validator for a floating point
     * @param key the key that is about to change
     * @param value the requested new value
     * @return true the value is ok, false the value is not ok
     */
    virtual bool validate(const std::string &key, float value) = 0;

    /**
     * Validator for a character string
     * @param key the key that is about to change
     * @param value the requested new value
     * @return true the value is ok, false the value is not ok
     */
    virtual bool validate(const std::string &key, const char *value) = 0;

    virtual ~ValueChangedValidator() { }
};


class SizeRangeValidator : public ValueChangedValidator {
public:
    SizeRangeValidator(size_t low, size_t high) : min(low), max(high) {}

    /**
     * Validator for a numeric value
     * @param key the key that is about to change
     * @param value the requested new value
     * @return true the value is ok, false the value is not ok
     */
    virtual bool validate(const std::string &key, size_t value) {
        (void)key;
        return (value >= min && value <= max);
    }

    /**
     * There is nothing I can do with this...
     */
    virtual bool validate(const std::string &, bool) {
        return false;
    }

    /**
     * Try to check if it's within the range..
     * @param value the requested new value
     * @return true the value is ok, false the value is not ok
     */
    virtual bool validate(const std::string &key, float value) {
        size_t val = static_cast<size_t>(value);
        return validate(key, val);
    }

    /**
     * Try to convert to size_t and see if it's within the legal range
     * @param key the key that is about to change
     * @param value the requested new value
     * @return true the value is ok, false the value is not ok
     */
    virtual bool validate(const std::string &key, const char *value) {
        if (value == NULL) {
            return false;
        }
        size_t val = static_cast<size_t>(atol(value));
        return validate(key, val);
    }

private:
    size_t min;
    size_t max;
};

class Configuration {
public:
    Configuration();
    ~Configuration();

    // Include the generated prototypes for the member functions
#include "generated_configuration.hh"

    /**
     * Parse a configuration string and set the local members
     *
     * @param str the string to parse
     * @param sapi pointer to the server API
     * @return true if success, false otherwise
     */
    bool parseConfiguration(const char *str, SERVER_HANDLE_V1* sapi);

    /**
     * Add all of the configuration variables as stats
     * @param add_stat the callback to add statistics
     * @param c the cookie for the connection who wants the stats
     */
    void addStats(ADD_STAT add_stat, const void *c) const;

    /**
     * Set the configuration parameter for a given key to
     * a boolean value.
     * @param key the key to specify
     * @param value the new value
     * @throws std::string if the value is refused by the validator
     */
    void setParameter(const std::string &key, bool value);
    /**
     * Set the configuration parameter for a given key to
     * a size_t value.
     * @param key the key to specify
     * @param value the new value
     * @throws std::string if the value is refused by the validator
     */
    void setParameter(const std::string &key, size_t value);
    /**
     * Set the configuration parameter for a given key to
     * a floating value.
     * @param key the key to specify
     * @param value the new value
     * @throws std::string if the value is refused by the validator
     */
    void setParameter(const std::string &key, float value);
    /**
     * Set the configuration parameter for a given key to
     * a character string
     * @param key the key to specify
     * @param value the new value
     * @throws std::string if the value is refused by the validator
     */
    void setParameter(const std::string &key, const char *value);
    /**
     * Set the configuration parameter for a given key to
     * a string.
     * @param key the key to specify
     * @param value the new value
     * @throws std::string if the value is refused by the validator
     */
    void setParameter(const std::string &key, const std::string &value);

    /**
     * Add a listener for changes for a key. The configuration class
     * will release the memory for the ValueChangedListner by calling
     * delete in it's destructor (so you have to allocate it by using
     * new). There is no way to remove a valueChangeListner.
     *
     * @param key the key to add the listener for
     * @param val the listener that will receive all of the callbacks
     *            when the value change.
     */
    void addValueChangedListener(const std::string &key,
                                 ValueChangedListener *val);

    /**
     * Set a validator for a specific key. The configuration class
     * will release the memory for the ValueChangedValidator by calling
     * delete in its destructor (so you have to allocate it by using
     * new). If a validator exists for the key, that will be returned
     * (and it's up to the caller to release the memory for that
     * validator).
     *
     * @param key the key to set the validator for
     * @param validator the new validator
     * @return the old validator (or NULL if there wasn't a validator)
     */
    ValueChangedValidator *setValueValidator(const std::string &key,
                                             ValueChangedValidator *validator);

private:
    void initialize();
    std::string getString(const std::string &key) const;
    bool getBool(const std::string &key) const;
    float getFloat(const std::string &key) const;
    size_t getInteger(const std::string &key) const;

    struct value_t {
        value_t() : validator(NULL) { val.v_string = 0; }
        std::vector<ValueChangedListener *> changeListener;
        ValueChangedValidator *validator;
        config_datatype datatype;
        union {
            size_t v_size;
            float v_float;
            bool v_bool;
            const char *v_string;
        } val;
    };

    // Access to the configuration variables is protected by the mutex
    Mutex mutex;
    std::map<std::string, value_t> attributes;

    friend std::ostream& operator<< (std::ostream& out,
                                     const Configuration &config);
};

#endif
