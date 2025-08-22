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

#include <folly/Synchronized.h>
#include <memcached/config_parser.h>
#include <memcached/server_core_iface.h>
#include <platform/cb_malloc.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <utility>

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

static nlohmann::json to_json(const value_variant_t& value) {
    return std::visit([](auto&& elem) { return nlohmann::json(elem); }, value);
}

void ValueChangedListener::booleanValueChanged(std::string_view key, bool) {
    logUnhandledType(key, "bool");
}

void ValueChangedListener::sizeValueChanged(std::string_view key, size_t) {
    logUnhandledType(key, "size_t");
}

void ValueChangedListener::ssizeValueChanged(std::string_view key, ssize_t) {
    logUnhandledType(key, "ssize_t");
}

void ValueChangedListener::floatValueChanged(std::string_view key, float) {
    logUnhandledType(key, "float");
}

void ValueChangedListener::stringValueChanged(std::string_view key,
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

void ValueChangedValidator::validateBool(std::string_view key, bool) {
    std::string error = "Configuration error.. " + std::string{key} +
                        " does not take a boolean parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateSize(std::string_view key, size_t) {
    std::string error = "Configuration error.. " + std::string{key} +
                        " does not take a size_t parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateSSize(std::string_view key, ssize_t) {
    std::string error = "Configuration error.. " + std::string{key} +
                        " does not take a ssize_t parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateFloat(std::string_view key, float) {
    std::string error = "Configuration error.. " + std::string{key} +
                        " does not take a float parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateString(std::string_view key, const char*) {
    std::string error = "Configuration error.. " + std::string{key} +
                        " does not take a string parameter";
    EP_LOG_DEBUG_RAW(error);
    throw std::runtime_error(error);
}

Configuration::Configuration(bool isServerless, bool isDevAssertEnabled)
    : isServerless(isServerless), isDevAssertEnabled(isDevAssertEnabled) {
    initialize();
    initialized = true;
}

struct Configuration::Attribute {
    explicit Attribute(bool dynamic) : dynamic(dynamic) {
    }

    /// The validator cannot be changed after initialization.
    std::unique_ptr<ValueChangedValidator> validator;
    /// The requirement cannot be changed after initialization.
    std::unique_ptr<Requirement> requirement;

    /// Is this parameter dynamic (can be changed at runtime?)
    const bool dynamic;

    value_variant_t getValue() const {
        return valueAndListeners.withRLock(
                [](auto& locked) { return locked.variant; });
    }

    [[nodiscard]] std::vector<std::shared_ptr<ValueChangedListener>> setValue(
            value_variant_t newValue) {
        valueAndListeners.withWLock([&newValue](auto& locked) {
            using std::swap;
            // Allows the std::string to be released outside the lock.
            swap(locked.variant, newValue);
        });
        // Copying the vector will be relatively expensive, so don't block other
        // readers.
        return valueAndListeners.withRLock(
                [](auto& locked) { return locked.changeListeners; });
    }

    void addChangeListener(std::shared_ptr<ValueChangedListener> listener) {
        valueAndListeners.withWLock([&listener](auto& locked) {
            locked.changeListeners.emplace_back(std::move(listener));
        });
    }

private:
    struct Value {
        std::vector<std::shared_ptr<ValueChangedListener>> changeListeners;

        // At the moment, the order of these template parameters must
        // match the order of the types in config_datatype. Looking
        // for a cleaner method.
        value_variant_t variant;
    };

    /// Stores the current value and set of listeners for the attribute.
    /// Those are the only properties of an attribute which can be changed after
    /// initialization.
    folly::Synchronized<Value, std::shared_mutex> valueAndListeners;
};

std::unordered_set<std::string>
Configuration::getDynamicParametersForTesting() {
    Configuration config;
    std::unordered_set<std::string> dynamicParameters;
    for (const auto& [key, attribute] : config.attributes) {
        if (attribute->dynamic) {
            dynamicParameters.insert(key);
        }
    }
    return dynamicParameters;
}

template <class T>
void Configuration::addParameter(std::string_view key,
                                 T defaultVal,
                                 std::optional<T> defaultServerless,
                                 std::optional<T> defaultTSAN,
                                 std::optional<T> defaultDevAssert,
                                 bool dynamic) {
    Expects(!initialized);
    auto [itr, success] = attributes.insert(
            {std::string{key}, std::make_shared<Attribute>(dynamic)});
    if (!success) {
        throw std::logic_error("Configuration::addParameter(" +
                               std::string{key} + ") already exists.");
    }
    if (folly::kIsSanitizeThread && defaultTSAN.has_value()) {
        (void)itr->second->setValue(*defaultTSAN);
    } else if (isDevAssertEnabled && defaultDevAssert.has_value()) {
        (void)itr->second->setValue(*defaultDevAssert);
    } else if (isServerless && defaultServerless.has_value()) {
        (void)itr->second->setValue(*defaultServerless);
    } else {
        (void)itr->second->setValue(defaultVal);
    }
}

template <class T>
void Configuration::setParameter(std::string_view key, T value) {
    Expects(initialized);

    auto it = attributes.find(key);
    if (it == attributes.end()) {
        throw std::invalid_argument("Configuration::setParameter(" +
                                    std::string{key} + ") doesn't exist.");
    }

    if (it->second->validator) {
        it->second->validator->validate(key, value);
    };

    for (const auto& listener : it->second->setValue(value)) {
        listener->valueChanged(key, value);
    }
}

template <>
void Configuration::setParameter<const char*>(std::string_view key,
                                              const char* value) {
    Expects(initialized);
    setParameter(key, std::string(value));
}

template <class T>
T Configuration::getParameter(std::string_view key) const {
    Expects(initialized);
    const auto iter = attributes.find(key);
    if (iter == attributes.end()) {
        return T();
    }

    const auto variant = iter->second->getValue();
    auto* value = std::get_if<T>(&variant);

    if (!value) {
        throw std::invalid_argument("Configuration::getParameter: key \"" +
                                    std::string{key} + "\" (which is " +
                                    to_string(variant) + ") is not " +
                                    type_name<T>::value);
    }
    return *value;
}

template bool Configuration::getParameter<bool>(std::string_view key) const;
template size_t Configuration::getParameter<size_t>(std::string_view key) const;
template ssize_t Configuration::getParameter<ssize_t>(
        std::string_view key) const;
template float Configuration::getParameter<float>(std::string_view key) const;
template std::string Configuration::getParameter<std::string>(
        std::string_view key) const;

void Configuration::maybeAddStat(const BucketStatCollector& collector,
                                 cb::stats::Key key,
                                 std::string_view keyStr) const {
    Expects(initialized);
    auto itr = attributes.find(keyStr);
    if (itr == attributes.end()) {
        return;
    }
    const auto& attribute = itr->second;
    if (!requirementsMet(*attribute)) {
        return;
    }

    auto variant = attribute->getValue();
    std::visit(
            [&collector, &key](auto&& elem) { collector.addStat(key, elem); },
            variant);
}

std::ostream& operator<<(std::ostream& out, const Configuration& config) {
    Expects(config.initialized);
    for (const auto& attribute : config.attributes) {
        std::stringstream line;
        {
            const auto variant = attribute.second->getValue();
            line << attribute.first.c_str() << " = [" << to_string(variant)
                 << "]" << std::endl;
        }
        out << line.str();
    }

    return out;
}

void Configuration::addAlias(const std::string& key, const std::string& alias) {
    Expects(!initialized);
    attributes[alias] = attributes[key];
}

void Configuration::addValueChangedListener(
        std::string_view key, std::unique_ptr<ValueChangedListener> val) {
    Expects(initialized);
    auto it = attributes.find(key);
    if (it == attributes.end()) {
        throw std::invalid_argument(
                fmt::format("Configuration::addValueChangedListener: No such "
                            "config key '{}'",
                            key));
    }

    it->second->addChangeListener(std::move(val));
}

Configuration::Configuration(const Configuration& other)
    : isServerless(other.isServerless),
      isDevAssertEnabled(other.isDevAssertEnabled) {
    initialize();
    initialized = true;
    for (const auto& [key, value] : other.attributes) {
        (void)attributes[key]->setValue(value->getValue());
    }
}

ValueChangedValidator* Configuration::setValueValidator(
        std::string_view key, ValueChangedValidator* validator) {
    Expects(!initialized);
    auto it = attributes.find(key);
    if (it == attributes.end()) {
        return nullptr;
    }
    auto* ret = it->second->validator.release();
    it->second->validator.reset(validator);

    return ret;
}

Requirement* Configuration::setRequirements(const std::string& key,
                                            Requirement* requirement) {
    Expects(!initialized);
    Requirement* ret = nullptr;
    if (attributes.contains(key)) {
        ret = attributes[key]->requirement.release();
        attributes[key]->requirement.reset(requirement);
    }

    return ret;
}

bool Configuration::requirementsMet(const Attribute& value) const {
    Expects(initialized);
    if (value.requirement) {
        for (auto requirement : value.requirement->requirements) {
            const auto iter = attributes.find(requirement.first);
            if (iter == attributes.end()) {
                // Parameter does not exist, returning true assuming the config
                // is not yet complete. We cannot verify yet.
                return true;
            }
            if (iter->second->getValue() != requirement.second) {
                return false;
            }
        }
    }
    return true;
}
void Configuration::requirementsMetOrThrow(std::string_view key) const {
    Expects(initialized);
    auto itr = attributes.find(key);
    if (itr != attributes.end()) {
        if (!requirementsMet(*itr->second)) {
            throw requirements_unsatisfied("Cannot set" + std::string{key} +
                                           " : requirements not met");
        }
    }
}

/**
 * Parses the value into the variant, preserving the type of the variant.
 *
 * @param value The value to parse.
 * @param variant The variant to modify.
 * @throws std::runtime_error if the value cannot be parsed into the variant.
 */
static void parseParameter(const std::string& value, value_variant_t& variant) {
    enum config_datatype { DT_SIZE, DT_SSIZE, DT_FLOAT, DT_BOOL, DT_STRING };
    switch (config_datatype(variant.index())) {
    case DT_STRING:
        variant = value;
        break;
    case DT_SIZE:
        variant = cb::config::value_as_size_t(value);
        break;
    case DT_SSIZE:
        variant = cb::config::value_as_ssize_t(value);
        break;
    case DT_BOOL:
        variant = cb::config::value_as_bool(value);
        break;
    case DT_FLOAT:
        variant = cb::config::value_as_float(value);
        break;
    }
}

bool Configuration::parseConfiguration(std::string_view str) {
    Expects(initialized);
    enum config_datatype { DT_SIZE, DT_SSIZE, DT_FLOAT, DT_BOOL, DT_STRING };

    bool failed = false;
    cb::config::tokenize(str, [&failed, this](auto k, auto v) {
        bool found = false;
        for (const auto& [key, value] : attributes) {
            if (k == key) {
                found = true;
                try {
                    auto newValue = value->getValue();
                    parseParameter(v, newValue);
                    std::visit([this, k](auto&& val) { setParameter(k, val); },
                               newValue);
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

    if (!failed) {
        // Now do the conditional init, which is last so it can read all current
        // state.
        runConditionalInitialize();
    }

    return !failed;
}

void Configuration::visit(Configuration::Visitor visitor) const {
    Expects(initialized);
    for (const auto& attr : attributes) {
        if (requirementsMet(*attr.second)) {
            visitor(attr.first,
                    attr.second->dynamic,
                    to_string(attr.second->getValue()));
        }
    }
}

void Configuration::parseAndSetParameter(std::string_view key,
                                         std::string_view value) {
    Expects(initialized);
    requirementsMetOrThrow(key);

    auto it = attributes.find(key);
    if (it == attributes.end()) {
        throw std::invalid_argument("Unknown config param '" +
                                    std::string{key} + "'");
    }

    if (!it->second->dynamic) {
        throw std::logic_error("Parameter '" + std::string{key} +
                               "' is not dynamic");
    }

    auto newValue = it->second->getValue();
    parseParameter(std::string(value), newValue);
    std::visit([this, key](auto&& val) { setParameter(key, val); }, newValue);
}

ParameterValidationMap Configuration::validateParameters(
        const ParameterMap& parameters) const {
    // Validate against a copy of this configuration.
    Configuration config(*this);
    auto [map, success] = config.setParametersInternal(parameters);

    if (success) {
        // Now do the conditional init, which is last so it can read all current
        // state.
        config.runConditionalInitialize();

        // Fill in the defaults for parameters that were not set.
        config.fillDefaults(map);
    }

    return map;
}

void Configuration::fillDefaults(ParameterValidationMap& map) const {
    for (const auto& [key, attr] : attributes) {
        if (map.find(key) == map.end() && requirementsMet(*attr)) {
            map.emplace(
                    key,
                    ParameterInfo(to_json(attr->getValue()), !attr->dynamic));
        }
    }
}

std::pair<ParameterValidationMap, bool> Configuration::setParametersInternal(
        const ParameterMap& parameters) {
    ParameterValidationMap result;

    bool failed = false;

    for (const auto& [key, value] : parameters) {
        auto it = attributes.find(key);
        if (it == attributes.end()) {
            result.emplace(key, ParameterError::unsupported());
            continue;
        }

        const auto& attribute = *it->second;
        auto newValue = attribute.getValue();
        try {
            parseParameter(value, newValue);
            std::visit([this, key](auto&& val) { setParameter(key, val); },
                       newValue);
        } catch (const std::exception& e) {
            result.emplace(key, ParameterError::invalidValue(e.what()));
            failed = true;
            continue;
        }

        // We've successfully set the parameter.
        result.emplace(key,
                       ParameterInfo(to_json(newValue), !attribute.dynamic));
    }

    // Check the all requirements are met - note this may override the success
    // ParameterInfo for a parameter if the requirements are not met.
    for (const auto& [key, value] : parameters) {
        auto it = attributes.find(key);
        if (it != attributes.end()) {
            if (!requirementsMet(*it->second)) {
                result.at(key) = ParameterError::invalidValue(
                        "Parameter requirements not met");
                failed = true;
            }
        }
    }

    return {result, !failed};
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
    void forwardToCallable(std::string_view key, ArgType value) {
        if constexpr (std::is_invocable_v<Callback, ArgType>) {
            callback(value);
        } else {
            // Log that this isn't right, the listener doesn't handle the type
            // of this config param (same as ValueChangedListener default
            // behaviour)
            logUnhandledType(key, type_name<ArgType>::value);
        }
    }
    void booleanValueChanged(std::string_view key, bool value) override {
        forwardToCallable(key, value);
    }
    void sizeValueChanged(std::string_view key, size_t value) override {
        forwardToCallable(key, value);
    }
    void ssizeValueChanged(std::string_view key, ssize_t value) override {
        forwardToCallable(key, value);
    }
    void floatValueChanged(std::string_view key, float value) override {
        forwardToCallable(key, value);
    }
    void stringValueChanged(std::string_view key, const char* value) override {
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
void Configuration::addValueChangedFunc(std::string_view key,
                                        std::function<void(Arg)> callback) {
    Expects(initialized);
    owning_type_t<Arg> currentValue;
    {
        auto itr = attributes.find(key);
        if (itr == attributes.end()) {
            throw std::invalid_argument(
                    "Configuration::addValueChangedFunc: No such config key '" +
                    std::string{key} + "'");
        }

        // Config params will _always_ have a value of the intended type set,
        // either the default or some updated value.
        // By trying to get the value as type Arg, we can verify that the given
        // callback actually handles the correct type
        // e.g., user is not providing a callback handling string types for a
        // param with type size_t
        const auto& valueVariant = itr->second->getValue();
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

    auto itr = attributes.find(key);
    if (itr == attributes.end()) {
        throw std::invalid_argument(
                fmt::format("Configuration::addValueChangedFunc: No such "
                            "config key '{}'",
                            key));
    }
    // re-acquire the lock and insert the callback
    itr->second->addChangeListener(
            std::make_unique<ValueChangedCallback<Arg>>(std::move(callback)));
}

template void Configuration::addValueChangedFunc(std::string_view,
                                                 std::function<void(bool)>);
template void Configuration::addValueChangedFunc(std::string_view,
                                                 std::function<void(size_t)>);
template void Configuration::addValueChangedFunc(std::string_view,
                                                 std::function<void(ssize_t)>);
template void Configuration::addValueChangedFunc(std::string_view,
                                                 std::function<void(float)>);
template void Configuration::addValueChangedFunc(
        std::string_view, std::function<void(std::string_view)>);

// Explicit instantiations for addParameter for supported types.
#define INSTANTIATE_TEMPLATES(T)                                \
    template void Configuration::addParameter(std::string_view, \
                                              T,                \
                                              std::optional<T>, \
                                              std::optional<T>, \
                                              std::optional<T>, \
                                              bool)

INSTANTIATE_TEMPLATES(bool);
INSTANTIATE_TEMPLATES(size_t);
INSTANTIATE_TEMPLATES(ssize_t);
INSTANTIATE_TEMPLATES(float);
INSTANTIATE_TEMPLATES(std::string);

#undef INSTANTIATE_TEMPLATES
