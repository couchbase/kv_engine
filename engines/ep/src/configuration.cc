/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "configuration.h"
#include "bucket_logger.h"
#include "configuration_impl.h"

#include <platform/cb_malloc.h>
#include <statistics/labelled_collector.h>
#include <memcached/config_parser.h>
#include <memcached/server_core_iface.h>
#include <statistics/cbstat_collector.h>
#include <sstream>

// Used to get a name from a type to use in logging
template <typename T>
struct type_name {
    static const char* const value;
};

#define TYPENAME(type) \
    template <>        \
    const char* const type_name<type>::value = #type;

TYPENAME(bool)
TYPENAME(size_t)
TYPENAME(ssize_t)
TYPENAME(float)
TYPENAME(std::string)
TYPENAME(std::string_view)
#undef TYPENAME

std::string to_string(const value_variant_t& value) {
    // Due to the way fmtlib and std::variant interact, if the variant
    // contains a bool type it will get converted to a integer type
    // and hence printed as '0' or '1' instead of 'false' / 'true'.
    // Avoid this by explicitly printing as a bool type if that's
    // what the variant contains.
    if (auto* bool_ptr = std::get_if<bool>(&value)) {
        return fmt::format("{}", *bool_ptr);
    }
    return std::visit([](auto&& elem) { return fmt::format("{}", elem); },
                      value);
}

void ValueChangedListener::booleanValueChanged(const std::string& key, bool) {
    logUnhandledType(key, "bool");
}

void ValueChangedListener::sizeValueChanged(const std::string& key, size_t) {
    logUnhandledType(key, "size_t");
}

void ValueChangedListener::ssizeValueChanged(const std::string& key, ssize_t) {
    logUnhandledType(key, "ssize_t");
}

void ValueChangedListener::floatValueChanged(const std::string& key, float) {
    logUnhandledType(key, "float");
}

void ValueChangedListener::stringValueChanged(const std::string& key,
                                              const char*) {
    logUnhandledType(key, "string");
}
void ValueChangedListener::logUnhandledType(std::string_view key,
                                            std::string_view type) {
    EP_LOG_DEBUG(
            "Configuration error. Listener for config key {} does not expect a "
            "value of type {}",
            key,
            type);
}

void ValueChangedValidator::validateBool(const std::string& key, bool) {
    std::string error = "Configuration error.. " + key +
                        " does not take a boolean parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateSize(const std::string& key, size_t) {
    std::string error = "Configuration error.. " + key +
                        " does not take a size_t parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateSSize(const std::string& key, ssize_t) {
    std::string error = "Configuration error.. " + key +
                        " does not take a ssize_t parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateFloat(const std::string& key, float) {
    std::string error =
            "Configuration error.. " + key + " does not take a float parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateString(const std::string& key,
                                           const char*) {
    std::string error = "Configuration error.. " + key +
                        " does not take a string parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

Configuration::Configuration(bool isServerless) : isServerless(isServerless) {
    initialize();
}

struct Configuration::value_t {
    explicit value_t(bool dynamic) : dynamic(dynamic) {
    }

    std::vector<std::unique_ptr<ValueChangedListener>> changeListener;
    std::unique_ptr<ValueChangedValidator> validator;
    std::unique_ptr<Requirement> requirement;

    // At the moment, the order of these template parameters must
    // match the order of the types in config_datatype. Looking
    // for a cleaner method.
    value_variant_t value;

    /// Is this parameter dynamic (can be changed at runtime?)
    const bool dynamic;

    std::vector<ValueChangedListener*> copyListeners() {
        std::vector<ValueChangedListener*> copy;

        std::transform(
                changeListener.begin(),
                changeListener.end(),
                std::back_inserter(copy),
                [](std::unique_ptr<ValueChangedListener>& listener)
                        -> ValueChangedListener* { return listener.get(); });
        return copy;
    }
};

template <class T>
void Configuration::addParameter(std::string key, T value, bool dynamic) {
    addParameter<T>(key, value, value, dynamic);
}

template <class T>
void Configuration::addParameter(std::string key,
                                 T defaultOnPrem,
                                 T defaultServerless,
                                 bool dynamic) {
    auto [itr, success] =
            attributes.insert({key, std::make_shared<value_t>(dynamic)});
    if (!success) {
        throw std::logic_error("Configuration::addParameter(" + key +
                               ") already exists.");
    }
    itr->second->value = isServerless ? defaultServerless : defaultOnPrem;
}

template <class T>
void Configuration::setParameter(const std::string& key, T value) {
    std::vector<ValueChangedListener*> copy;
    {
        std::lock_guard<std::mutex> lh(mutex);
        auto it = attributes.find(key);
        if (it == attributes.end()) {
            throw std::invalid_argument("Configuration::setParameter(" + key +
                                        ") doesn't exist.");
        }
        if (it->second->validator) {
            it->second->validator->validate(key, value);
        }
        it->second->value = value;

        // Take a copy of the listeners so we can call them without holding
        // the mutex.
        copy = it->second->copyListeners();
    }

    for (auto* listener : copy) {
        listener->valueChanged(key, value);
    }
}

template <>
void Configuration::setParameter<const char*>(const std::string& key,
                                              const char* value) {
    setParameter(key, std::string(value));
}

template <class T>
T Configuration::getParameter(const std::string& key) const {
    std::lock_guard<std::mutex> lh(mutex);

    const auto iter = attributes.find(key);
    if (iter == attributes.end()) {
        return T();
    }

    auto* value = std::get_if<T>(&iter->second->value);

    if (!value) {
        throw std::invalid_argument("Configuration::getParameter: key \"" +
                                    key + "\" (which is " +
                                    to_string(iter->second->value) +
                                    ") is not " + type_name<T>::value);
    }
    return *value;
}

template bool Configuration::getParameter<bool>(const std::string& key) const;
template size_t Configuration::getParameter<size_t>(
        const std::string& key) const;
template ssize_t Configuration::getParameter<ssize_t>(
        const std::string& key) const;
template float Configuration::getParameter<float>(const std::string& key) const;
template std::string Configuration::getParameter<std::string>(
        const std::string& key) const;

void Configuration::maybeAddStat(const BucketStatCollector& collector,
                                 cb::stats::Key key,
                                 std::string_view keyStr) const {
    auto itr = attributes.find(keyStr);
    if (itr == attributes.end()) {
        return;
    }
    const auto& attribute = itr->second;
    if (!requirementsMet(*attribute)) {
        return;
    }
    std::visit(
            [&collector, &key](auto&& elem) { collector.addStat(key, elem); },
            attribute->value);
}

std::ostream& operator<<(std::ostream& out, const Configuration& config) {
    std::lock_guard<std::mutex> lh(config.mutex);
    for (const auto& attribute : config.attributes) {
        std::stringstream line;
        line << attribute.first.c_str() << " = ["
             << to_string(attribute.second->value) << "]" << std::endl;
        out << line.str();
    }

    return out;
}

void Configuration::addAlias(const std::string& key, const std::string& alias) {
    attributes[alias] = attributes[key];
}

void Configuration::addValueChangedListener(
        const std::string& key, std::unique_ptr<ValueChangedListener> val) {
    std::lock_guard<std::mutex> lh(mutex);
    if (attributes.find(key) == attributes.end()) {
        throw std::invalid_argument(
                "Configuration::addValueChangedListener: No such config key '" +
                key + "'");
    }
    attributes[key]->changeListener.emplace_back(std::move(val));
}

ValueChangedValidator *Configuration::setValueValidator(const std::string &key,
                                            ValueChangedValidator *validator) {
    ValueChangedValidator *ret = nullptr;
    std::lock_guard<std::mutex> lh(mutex);
    if (attributes.find(key) != attributes.end()) {
        ret = attributes[key]->validator.release();
        attributes[key]->validator.reset(validator);
    }

    return ret;
}

Requirement* Configuration::setRequirements(const std::string& key,
                                            Requirement* requirement) {
    Requirement* ret = nullptr;
    std::lock_guard<std::mutex> lh(mutex);
    if (attributes.find(key) != attributes.end()) {
        ret = attributes[key]->requirement.release();
        attributes[key]->requirement.reset(requirement);
    }

    return ret;
}

bool Configuration::requirementsMet(const value_t& value) const {
    if (value.requirement) {
        for (auto requirement : value.requirement->requirements) {
            const auto iter = attributes.find(requirement.first);
            if (iter == attributes.end()) {
                // Parameter does not exist, returning true assuming the config
                // is not yet complete. We cannot verify yet.
                return true;
            }
            if (iter->second->value != requirement.second) {
                return false;
            }
        }
    }
    return true;
}
void Configuration::requirementsMetOrThrow(const std::string& key) const {
    std::lock_guard<std::mutex> lh(mutex);
    auto itr = attributes.find(key);
    if (itr != attributes.end()) {
        if (!requirementsMet(*(itr->second))) {
            throw requirements_unsatisfied("Cannot set" + key +
                                           " : requirements not met");
        }
    }
}

bool Configuration::parseConfiguration(std::string_view str) {
    enum config_datatype { DT_SIZE, DT_SSIZE, DT_FLOAT, DT_BOOL, DT_STRING };

    bool failed = false;
    cb::config::tokenize(str, [&failed, this](auto k, auto v) {
        bool found = false;
        for (const auto& [key, value] : attributes) {
            if (k == key) {
                found = true;
                try {
                    switch (config_datatype(value->value.index())) {
                    case DT_STRING:
                        setParameter(key, v);
                        break;
                    case DT_SIZE:
                        setParameter(key, cb::config::value_as_size_t(v));
                        break;
                    case DT_SSIZE:
                        setParameter(key, cb::config::value_as_ssize_t(v));
                        break;
                    case DT_BOOL:
                        setParameter(key, cb::config::value_as_bool(v));
                        break;
                    case DT_FLOAT:
                        setParameter(key, cb::config::value_as_float(v));
                        break;
                    }
                } catch (const std::exception& e) {
                    EP_LOG_WARN(
                            "Error parsing value: key: {} value: {} error: {}",
                            k,
                            v,
                            e.what());
                    failed = true;
                }
            }
        }
        if (!found) {
            EP_LOG_WARN("Unknown configuration key: {} value: {}", k, v);
        }
    });

    return !failed;
}

void Configuration::visit(Configuration::Visitor visitor) const {
    for (const auto& attr : attributes) {
        if (requirementsMet(*attr.second)) {
            visitor(attr.first,
                    attr.second->dynamic,
                    to_string(attr.second->value));
        }
    }
}

Configuration::~Configuration() = default;

/**
 * Listener notifying a provided callable when a config value has changed.
 */
template <class Arg>
class ValueChangedCallback : public ValueChangedListener {
public:
    using Callback = std::function<void(Arg)>;
    ValueChangedCallback(Callback cb) : callback(std::move(cb)) {
    }

    template <class ArgType>
    void forwardToCallable(const std::string& key, ArgType value) {
        if constexpr (std::is_invocable_v<Callback, ArgType>) {
            callback(value);
        } else {
            // Log that this isn't right, the listener doesn't handle the type
            // of this config param (same as ValueChangedListener default
            // behaviour)
            logUnhandledType(key, type_name<ArgType>::value);
        }
    }
    void booleanValueChanged(const std::string& key, bool value) override {
        forwardToCallable(key, value);
    }
    void sizeValueChanged(const std::string& key, size_t value) override {
        forwardToCallable(key, value);
    }
    void ssizeValueChanged(const std::string& key, ssize_t value) override {
        forwardToCallable(key, value);
    }
    void floatValueChanged(const std::string& key, float value) override {
        forwardToCallable(key, value);
    }
    void stringValueChanged(const std::string& key,
                            const char* value) override {
        forwardToCallable(key, std::string_view(value));
    }

private:
    Callback callback;
};

// map to owning type - config values are owning e.g., std::string
// but callbacks can instead take non-owning types e.g., std::string_view
template <class T>
struct owning_type {
    using type = T;
};

template <>
struct owning_type<std::string_view> {
    using type = std::string;
};

template <class T>
using owning_type_t = typename owning_type<T>::type;

template <class Arg>
void Configuration::addValueChangedFunc(const std::string& key,
                                        std::function<void(Arg)> callback) {
    owning_type_t<Arg> currentValue;
    {
        std::lock_guard<std::mutex> lh(mutex);
        auto itr = attributes.find(key);
        if (itr == attributes.end()) {
            throw std::invalid_argument(
                    "Configuration::addValueChangedFunc: No such config key '" +
                    key + "'");
        }
        // Config params will _always_ have a value of the intended type set,
        // either the default or some updated value.
        // By trying to get the value as type Arg, we can verify that the given
        // callback actually handles the correct type
        // e.g., user is not providing a callback handling string types for a
        // param with type size_t
        const auto& valueVariant = itr->second->value;
        auto* valuePtr = std::get_if<owning_type_t<Arg>>(&valueVariant);

        if (!valuePtr) {
            auto actualConfigType = std::visit(
                    [](auto v) { return type_name<decltype(v)>::value; },
                    valueVariant);
            throw std::invalid_argument(fmt::format(
                    "Configuration::addValueChangedFunc: Callback provided "
                    "which accepts {} instead of expected type {} for key '{}'",
                    type_name<Arg>::value,
                    actualConfigType,
                    key));
        }

        // copy out the current value
        currentValue = *valuePtr;
        // lock dropped here
    }

    // For most uses, a user will first wish to read the current config value,
    // do "something" with it, then register a listener to do that "thing" on
    // future changes. Given this is a standard pattern, just immediately invoke
    // the callback now. This means a caller can't forget to read the current
    // value, and adding lots of listeners is less verbose.

    // The listener must be called outside of the lock, as it may acquire the
    // config lock itself. This means acquiring and dropping the lock.
    // This could lead to a missed update to the config value, but listeners
    // are generally registered quite early in a bucket's life, before it
    // would be possible to change the config.
    // In any case, this has been acceptable for all existing usages, so
    // keep that pattern here.
    callback(currentValue);

    // re-acquire the lock and insert the callback
    std::lock_guard<std::mutex> lh(mutex);
    attributes[key]->changeListener.emplace_back(
            std::make_unique<ValueChangedCallback<Arg>>(std::move(callback)));
}

template void Configuration::addValueChangedFunc(const std::string&,
                                                 std::function<void(bool)>);
template void Configuration::addValueChangedFunc(const std::string&,
                                                 std::function<void(size_t)>);
template void Configuration::addValueChangedFunc(const std::string&,
                                                 std::function<void(ssize_t)>);
template void Configuration::addValueChangedFunc(const std::string&,
                                                 std::function<void(float)>);
template void Configuration::addValueChangedFunc(
        const std::string&, std::function<void(std::string_view)>);

// Explicit instantiations for addParameter for supported types.
template void Configuration::addParameter(std::string, bool, bool);
template void Configuration::addParameter(std::string, size_t, bool);
template void Configuration::addParameter(std::string, ssize_t, bool);
template void Configuration::addParameter(std::string, float, bool);
template void Configuration::addParameter(std::string, std::string, bool);
