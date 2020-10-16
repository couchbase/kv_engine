/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "configuration.h"
#include "bucket_logger.h"
#include "configuration_impl.h"
#include "locks.h"

#include <platform/cb_malloc.h>
#include <statistics/labelled_collector.h>

#ifdef AUTOCONF_BUILD
#include "generated_configuration.cc"
#endif

#include <memcached/config_parser.h>
#include <memcached/server_core_iface.h>
#include <statistics/cbstat_collector.h>

#include <sstream>

// Used to get a name from a type to use in logging
template <typename T>
struct type_name {
    static char* const value;
};

#define TYPENAME(type) \
    template <>        \
    char* const type_name<type>::value = (char* const) #type;

TYPENAME(bool)
TYPENAME(size_t)
TYPENAME(ssize_t)
TYPENAME(float)
TYPENAME(std::string)
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
    EP_LOG_DEBUG("Configuration error.. {} does not expect a boolean value",
                 key);
}

void ValueChangedListener::sizeValueChanged(const std::string& key, size_t) {
    EP_LOG_DEBUG("Configuration error.. {} does not expect a size value", key);
}

void ValueChangedListener::ssizeValueChanged(const std::string& key, ssize_t) {
    EP_LOG_DEBUG("Configuration error.. {} does not expect a size value", key);
}

void ValueChangedListener::floatValueChanged(const std::string& key, float) {
    EP_LOG_DEBUG(
            "Configuration error.. {} does not expect a floating point"
            "value",
            key);
}

void ValueChangedListener::stringValueChanged(const std::string& key,
                                              const char*) {
    EP_LOG_DEBUG("Configuration error.. {} does not expect a string value",
                 key);
}
void ValueChangedValidator::validateBool(const std::string& key, bool) {
    std::string error = "Configuration error.. " + key +
                        " does not take a boolean parameter";
    EP_LOG_DEBUG(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateSize(const std::string& key, size_t) {
    std::string error = "Configuration error.. " + key +
                        " does not take a size_t parameter";
    EP_LOG_DEBUG(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateSSize(const std::string& key, ssize_t) {
    std::string error = "Configuration error.. " + key +
                        " does not take a ssize_t parameter";
    EP_LOG_DEBUG(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateFloat(const std::string& key, float) {
    std::string error =
            "Configuration error.. " + key + " does not take a float parameter";
    EP_LOG_DEBUG(error);
    throw std::runtime_error(error);
}

void ValueChangedValidator::validateString(const std::string& key,
                                           const char*) {
    std::string error = "Configuration error.. " + key +
                        " does not take a string parameter";
    EP_LOG_DEBUG(error);
    throw std::runtime_error(error);
}

Configuration::Configuration() {
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
    auto result = attributes.insert(
            {std::move(key), std::make_shared<value_t>(dynamic)});
    if (!result.second) {
        throw std::logic_error("Configuration::addParameter(" + key +
                               ") already exists.");
    }
    result.first->second->value = value;
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
    LockHolder lh(mutex);

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

std::ostream& operator<<(std::ostream& out, const Configuration& config) {
    LockHolder lh(config.mutex);
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
    LockHolder lh(mutex);
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
    LockHolder lh(mutex);
    if (attributes.find(key) != attributes.end()) {
        ret = attributes[key]->validator.release();
        attributes[key]->validator.reset(validator);
    }

    return ret;
}

Requirement* Configuration::setRequirements(const std::string& key,
                                            Requirement* requirement) {
    Requirement* ret = nullptr;
    LockHolder lh(mutex);
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
    LockHolder lh(mutex);
    auto itr = attributes.find(key);
    if (itr != attributes.end()) {
        if (!requirementsMet(*(itr->second))) {
            throw requirements_unsatisfied("Cannot set" + key +
                                           " : requirements not met");
        }
    }
}

void Configuration::addStats(const BucketStatCollector& collector) const {
    LockHolder lh(mutex);

    const auto lookupAttr = [this](const std::string& keyStr) {
        // remove the "ep_" prefix from the stat key
        return attributes.at(keyStr.substr(3));
    };

    using namespace cb::stats;
    const auto addStat = [this, &collector](Key key, const auto& attribute) {
        if (!requirementsMet(*attribute)) {
            return;
        }
        std::visit([&collector,
                    &key](auto&& elem) { collector.addStat(key, elem); },
                   attribute->value);
    };

#define STAT(name, unit, family, ...) addStat(Key::name, lookupAttr(#name));
#include <stats_config.def.h>
#undef STAT
}

/**
 * Internal container of an engine parameter.
 */
class ConfigItem: public config_item {
public:
    ConfigItem(const char *theKey, config_datatype theDatatype) :
                                                                holder(nullptr) {
        key = theKey;
        datatype = theDatatype;
        value.dt_string = &holder;
    }

private:
    char *holder;
};

bool Configuration::parseConfiguration(const char* str, ServerApi* sapi) {
    std::vector<std::unique_ptr<ConfigItem> > config;

    for (const auto& attribute : attributes) {
        config.push_back(std::make_unique<ConfigItem>(
                attribute.first.c_str(),
                config_datatype(attribute.second->value.index())));
    }

    const int nelem = config.size();
    std::vector<config_item> items(nelem + 1);
    for (int ii = 0; ii < nelem; ++ii) {
        items[ii].key = config[ii]->key;
        items[ii].datatype = config[ii]->datatype;
        items[ii].value.dt_string = config[ii]->value.dt_string;
    }

    bool ret = sapi->core->parse_config(str, items.data(), stderr) == 0;
    for (int ii = 0; ii < nelem; ++ii) {
        if (items[ii].found) {
            if (ret) {
                switch (items[ii].datatype) {
                case DT_STRING:
                    setParameter(items[ii].key,
                                 const_cast<const char*>(
                                         *(items[ii].value.dt_string)));
                    break;
                case DT_SIZE:
                    setParameter(items[ii].key, *items[ii].value.dt_size);
                    break;
                case DT_SSIZE:
                    setParameter(items[ii].key,
                                 (ssize_t)*items[ii].value.dt_ssize);
                    break;
                case DT_BOOL:
                    setParameter(items[ii].key, *items[ii].value.dt_bool);
                    break;
                case DT_FLOAT:
                    setParameter(items[ii].key, *items[ii].value.dt_float);
                    break;
                case DT_CONFIGFILE:
                    throw std::logic_error("Configuration::parseConfiguration: "
                            "Unexpected DT_CONFIGFILE element after parse_config");
                    break;
                }
            }

            if (items[ii].datatype == DT_STRING) {
                cb_free(*items[ii].value.dt_string);
            }
        }
    }

    return ret;
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

// Explicit instantiations for addParameter for supported types.
template void Configuration::addParameter(std::string, bool, bool);
template void Configuration::addParameter(std::string, size_t, bool);
template void Configuration::addParameter(std::string, ssize_t, bool);
template void Configuration::addParameter(std::string, float, bool);
template void Configuration::addParameter(std::string, std::string, bool);
