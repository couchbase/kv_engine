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

#include <memcached/configuration_iface.h>
#include <memcached/engine.h>
#include <relaxed_atomic.h>
#include <iostream>
#include <map>
#include <string>

#include "configuration_types.h"

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
    void valueChanged(std::string_view key, bool value) {
        booleanValueChanged(key, value);
    }

    void valueChanged(std::string_view key, size_t value) {
        sizeValueChanged(key, value);
    }

    void valueChanged(std::string_view key, ssize_t value) {
        ssizeValueChanged(key, value);
    }

    void valueChanged(std::string_view key, float value) {
        floatValueChanged(key, value);
    }

    void valueChanged(std::string_view key, std::string value) {
        stringValueChanged(key, value.c_str());
    }

    void valueChanged(std::string_view key, const char* value) {
        stringValueChanged(key, value);
    }

    /**
     * Callback if when a boolean configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void booleanValueChanged(std::string_view key, bool);

    /**
     * Callback if when a numeric configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void sizeValueChanged(std::string_view key, size_t);

    /**
     * Callback if when a numeric configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void ssizeValueChanged(std::string_view key, ssize_t);

    /**
     * Callback if when a floatingpoint configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void floatValueChanged(std::string_view key, float);
    /**
     * Callback if when a string configuration value changed
     * @param key the key who changed
     * @param value the new value for the key
     */
    virtual void stringValueChanged(std::string_view key, const char*);

    static void logUnhandledType(std::string_view key, std::string_view type);

    virtual ~ValueChangedListener() = default;
};

/**
 * The validator for the values runs with the mutex held
 * for the configuration class, so you can't try to access
 * any other configuration variables from the callback
 */
class ValueChangedValidator {
public:
    void validate(std::string_view key, bool value) {
        validateBool(key, value);
    }

    void validate(std::string_view key, size_t value) {
        validateSize(key, value);
    }

    void validate(std::string_view key, ssize_t value) {
        validateSSize(key, value);
    }

    void validate(std::string_view key, float value) {
        validateFloat(key, value);
    }

    void validate(std::string_view key, const char* value) {
        validateString(key, value);
    }
    void validate(std::string_view key, std::string value) {
        validateString(key, value.c_str());
    }

    /**
     * Validator for boolean values
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateBool(std::string_view key, bool);

    /**
     * Validator for a numeric value
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateSize(std::string_view key, size_t);

    /**
     * Validator for a signed numeric value
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateSSize(std::string_view key, ssize_t);

    /**
     * Validator for a floating point
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateFloat(std::string_view key, float);

    /**
     * Validator for a character string
     * @param key the key that is about to change
     * @param value the requested new value
     * @throws runtime_error if the validation failed
     */
    virtual void validateString(std::string_view key, const char*);

    virtual ~ValueChangedValidator() = default;
};

class StatCollector;
class Requirement;

/**
 * The configuration class represents and provides access to the
 * entire configuration of the server.
 */
class Configuration : public ConfigurationIface {
public:
    struct Attribute;

    /**
     * Ctor for Configuration
     * @param isServerless if true serverless default parameters will be stored
     * instead of on-prem default parameters
     */
    explicit Configuration(bool isServerless = false,
                           bool isDevAssertEnabled = false);
    ~Configuration() override;

    // Include the generated prototypes for the member functions
#include "generated_configuration.h" // NOLINT(*)

    /**
     * Parse a configuration string and set the local members
     *
     * @param str the string to parse
     * @return true if success, false otherwise
     */
    bool parseConfiguration(std::string_view str);

    /**
     * Add all of the configuration variables as stats
     * @param collector where to store collected statistics
     */
    void addStats(const BucketStatCollector& collector) const;

    using Visitor = std::function<void(
            std::string_view key, bool isDynamic, std::string value)>;

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
     * Unlike addAndNotifyValueChangedCallback, the listener is _not_
     * immediately notified of the current value of the config, for backwards
     * compatibility.
     *
     * TODO: inspect all uses to verify it is safe/correct to make this method
     * immediately invoke the listener like addAndNotifyValueChangedCallback
     * does.
     *
     * @param key the key to add the listener for
     * @param val the listener that will receive all of the callbacks
     *            when the value change.
     * @throw invalid_argument if the specified key isn't a valid config key.
     */
    void addValueChangedListener(std::string_view key,
                                 std::unique_ptr<ValueChangedListener> val);

    /**
     * Add a listener for changes for a key. There is no way to remove a
     * ValueChangeListener.
     *
     * If the parameter type is not handled by the provided callback,
     * std::invalid_argument will be thrown
     *
     * If the parameter exists and has the expected type, the listener will
     * immediately be notified of the current value.
     *
     *
     * @param key the config key the listener is interested in
     * @param val the listener which will be called when the config value
     *            changes
     * @throw invalid_argument if the specified key isn't a valid config key,
     *                         or if the callback does not handle the correct
     *                         type (e.g., config is size_t, callback takes
     *                         std::string_view).
     */
    template <class Callable>
    void addAndNotifyValueChangedCallback(std::string_view key,
                                          Callable&& callback) {
        addValueChangedFunc(key,
                            std::function(std::forward<Callable>(callback)));
    }

    /**
     * Throw an exception if the requirements are not met
     *
     * @param key the key to check
     * @throws logic_error if the requirements are not met
     */
    void requirementsMetOrThrow(std::string_view key) const;

    /**
     * Parse a parameter value, validate it and set it in the configuration.
     *
     * @param key the key to set
     * @param value the value to set
     * @throws std::logic_error if the requirements are not met
     * @throws std::logic_error if the key is not dynamic
     * @throws std::invalid_argument if the key is not found
     * @throws std::runtime_error if the value cannot be parsed
     */
    void parseAndSetParameter(std::string_view key, std::string_view value);

    /**
     * Get the dynamic parameters for the configuration (for testing purposes)
     */
    static std::unordered_set<std::string> getDynamicParametersForTesting();

    ParameterValidationMap validateParameters(
            const ParameterMap& parameters) const override;

    const bool isServerless;

    const bool isDevAssertEnabled = false;

protected:
    /**
     * Copy constructor.
     *
     * Note only the values are copied. Listeners are not copied.
     *
     * @param other The configuration to copy
     */
    Configuration(const Configuration& other);

    std::pair<ParameterValidationMap, bool> setParametersInternal(
            const ParameterMap& parameters);

    void fillDefaults(ParameterValidationMap& map) const;

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
    ValueChangedValidator* setValueValidator(std::string_view key,
                                             ValueChangedValidator* validator);

    /**
     * Adds a prerequisite to a configuration option. This must be satisfied
     * in order to set/get the config value or for it to appear in stats.
     *
     * @param key the key to set the requirement for
     * @param requirement the requirement
     */
    Requirement* setRequirements(const std::string& key,
                                 Requirement* requirement);

    bool requirementsMet(const Attribute& value) const;

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
     * Add a new configuration parameter that has different defaults values for
     * on-prem and serverless (size_t, ssize_t, float, bool, string)
     * @param key the key to specify
     * @param defaultVal default value - if no other defaults are specified
     * @param defaultServerless default value if we're running in an serverless
     *        context
     * @param defaultTSAN optional override if compiled with thread-sanitizer
     * @param defaultDevAssert optional override if compiled with
     * CB_DEVELOPMENT_ASSERTS
     * @param dynamic True if this parameter can be changed at runtime,
     *        False if the value cannot be changed once object is constructed.
     */
    template <class T>
    void addParameter(std::string_view key,
                      T defaultVal,
                      std::optional<T> defaultServerless,
                      std::optional<T> defaultTSAN,
                      std::optional<T> defaultDevAssert,
                      bool dynamic);

    /**
     * Set the configuration parameter for a given key to
     * a new value (size_t, ssize_t, float, bool, string)
     * @param key the key to specify
     * @param value the new value
     * @throws invalid_argument if the given key doesn't exist.
     * @throws runtime_error if the validation failed
     */
    template <class T>
    void setParameter(std::string_view key, T value);

    /**
     * Get the configuration parameter for a given key
     * @param key the key to specify
     * @return value the value
     * @throws runtime_error if the validation failed
     */
    template <class T>
    T getParameter(std::string_view key) const;

    /**
     * Add a single config param to the provided stat collector, if the
     * config requirements are met.
     */
    void maybeAddStat(const BucketStatCollector& collector,
                      cb::stats::Key key,
                      std::string_view keyStr) const;

    /**
     * Has the Configuration been initialized? Once initialized, the set of
     * attributes cannot be changed.
     */
    cb::RelaxedAtomic<bool> initialized{false};

    void initialize();

    /**
     * Run any generated conditional init statements.
     */
    void runConditionalInitialize();

    /**
     * Add a listener calling the provided std::function when the value for the
     * given config key changes.
     */
    template <class Arg>
    void addValueChangedFunc(std::string_view key,
                             std::function<void(Arg)> callback);

    std::map<std::string, std::shared_ptr<Attribute>, std::less<>> attributes;

    friend std::ostream& operator<< (std::ostream& out,
                                    const Configuration& config);
};

// This specialisation is needed to convert char* to std::string to store in
// the variant.
template <>
void Configuration::setParameter<const char*>(std::string_view key,
                                              const char* value);
