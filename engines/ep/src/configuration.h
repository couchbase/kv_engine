/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine.h>

#include <iostream>
#include <map>
#include <mutex>
#include <string>

// forward decl

namespace cb::stats {
enum class Key;
}

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
    void valueChanged(const std::string& key, bool value) {
        booleanValueChanged(key, value);
    }

    void valueChanged(const std::string& key, size_t value) {
        sizeValueChanged(key, value);
    }

    void valueChanged(const std::string& key, ssize_t value) {
        ssizeValueChanged(key, value);
    }

    void valueChanged(const std::string& key, float value) {
        floatValueChanged(key, value);
    }

    void valueChanged(const std::string& key, std::string value) {
        stringValueChanged(key, value.c_str());
    }

    void valueChanged(const std::string& key, const char* value) {
        stringValueChanged(key, value);
    }

    /**
     * Callback if when a boolean configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void booleanValueChanged(const std::string& key, bool);

    /**
     * Callback if when a numeric configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void sizeValueChanged(const std::string& key, size_t);

    /**
     * Callback if when a numeric configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void ssizeValueChanged(const std::string& key, ssize_t);

    /**
     * Callback if when a floatingpoint configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void floatValueChanged(const std::string& key, float);
    /**
     * Callback if when a string configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void stringValueChanged(const std::string& key, const char*);

    virtual ~ValueChangedListener() { /* EMPTY */}
};

/**
 * The validator for the values runs with the mutex held
 * for the configuration class, so you can't try to access
 * any other configuration variables from the callback
 */
class ValueChangedValidator {
public:
    void validate(const std::string& key, bool value) {
        validateBool(key, value);
    }

    void validate(const std::string& key, size_t value) {
        validateSize(key, value);
    }

    void validate(const std::string& key, ssize_t value) {
        validateSSize(key, value);
    }

    void validate(const std::string& key, float value) {
        validateFloat(key, value);
    }

    void validate(const std::string& key, const char* value) {
        validateString(key, value);
    }
    void validate(const std::string& key, std::string value) {
        validateString(key, value.c_str());
    }

    /**
     * Validator for boolean values
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateBool(const std::string& key, bool);

    /**
     * Validator for a numeric value
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateSize(const std::string& key, size_t);

    /**
     * Validator for a signed numeric value
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateSSize(const std::string& key, ssize_t);

    /**
     * Validator for a floating point
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateFloat(const std::string& key, float);

    /**
     * Validator for a character string
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateString(const std::string& key, const char*);

    virtual ~ValueChangedValidator() { }
};

class StatCollector;
class Requirement;

/**
 * The configuration class represents and provides access to the
 * entire configuration of the server.
 */
class Configuration {
public:
    struct value_t;

    Configuration();
    ~Configuration();

    // Include the generated prototypes for the member functions
#include "generated_configuration.h" // NOLINT(*)

    /**
     * Parse a configuration string and set the local members
     *
     * @param str the string to parse
     * @param sapi pointer to the server API
     * @return true if success, false otherwise
     */
    bool parseConfiguration(const char* str, ServerApi* sapi);

    /**
     * Add all of the configuration variables as stats
     * @param collector where to store collected statistics
     */
    void addStats(const BucketStatCollector& collector) const;

    using Visitor = std::function<void(
            const std::string& key, bool isDynamic, std::string value)>;

    /**
     * Invokes the given function on all configuration values which have
     * their requirements met.
     */
    void visit(Visitor visitor) const;

    /**
     * Add a listener for changes for a key. The configuration class takes
     * ownership of the listener. There is no way to remove a
     * ValueChangeListener.
     *
     * @param key the key to add the listener for
     * @param val the listener that will receive all of the callbacks
     *            when the value change.
     * @throw invalid_argument if the specified key isn't a valid config key.
     */
    void addValueChangedListener(const std::string& key,
                                 std::unique_ptr<ValueChangedListener> val);

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
    /**
     * Adds an alias for a configuration. Values can be set in configuration
     * under the original or aliased named, but setters/getters will only be
     * generated for the main name.
     *
     * @param key the key to which the alias refers
     * @param alias the new alias
     */
    void addAlias(const std::string& key, const std::string& alias);

    /**
     * Adds a prerequisite to a configuration option. This must be satisfied
     * in order to set/get the config value or for it to appear in stats.
     *
     * @param key the key to set the requirement for
     * @param requirement the requirement
     */
    Requirement* setRequirements(const std::string& key,
                                 Requirement* requirement);

    bool requirementsMet(const value_t& value) const;

    void requirementsMetOrThrow(const std::string& key) const;

protected:
    /**
     * Add a new configuration parameter (size_t, ssize_t, float, bool, string)
     * @param key the key to specify
     * @param value the new value
     * @param dynamic True if this parameter can be changed at runtime,
     *        False if the value cannot be changed once object is constructed.
     */
    template <class T>
    void addParameter(std::string key, T value, bool dynamic);

    /**
     * Set the configuration parameter for a given key to
     * a new value (size_t, ssize_t, float, bool, string)
     * @param key the key to specify
     * @param value the new value
     * @throws invalid_argument if the given key doesn't exist.
     * @throws runtime_error if the validation failed
     */
    template <class T>
    void setParameter(const std::string& key, T value);

    /**
     * Get the configuration parameter for a given key
     * @param key the key to specify
     * @return value the value
     * @throws runtime_error if the validation failed
     */
    template <class T>
    T getParameter(const std::string& key) const;

    /**
     * Add a single config param to the provided stat collector, if the
     * config requirements are met.
     */
    void maybeAddStat(const BucketStatCollector& collector,
                      cb::stats::Key key,
                      std::string_view keyStr) const;

private:
    void initialize();

    // Access to the configuration variables is protected by the mutex
    mutable std::mutex mutex;
    std::map<std::string, std::shared_ptr<value_t>, std::less<>> attributes;

    friend std::ostream& operator<< (std::ostream& out,
                                     const Configuration &config);
};

// This specialisation is needed to convert char* to std::string to store in
// the variant.
template <>
void Configuration::setParameter<const char*>(const std::string& key,
                                              const char* value);
