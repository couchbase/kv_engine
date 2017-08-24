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

#include "config.h"
#include "configuration.h"

#include "configuration_impl.h"

#include "locks.h"

#ifdef AUTOCONF_BUILD
#include "generated_configuration.cc"
#endif
#include "statwriter.h"

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

Configuration::Configuration() {
    initialize();
}

struct Configuration::value_t {
    std::vector<std::unique_ptr<ValueChangedListener>> changeListener;
    std::unique_ptr<ValueChangedValidator> validator;
    std::unique_ptr<Requirement> requirement;

    // At the moment, the order of these template parameters must
    // match the order of the types in config_datatype. Looking
    // for a cleaner method.
    value_variant_t value;

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
void Configuration::setParameter(const std::string& key, T value) {
    std::vector<ValueChangedListener*> copy;
    {
        std::lock_guard<std::mutex> lh(mutex);
        auto validator = attributes.find(key);
        if (validator != attributes.end()) {
            if (validator->second->validator) {
                validator->second->validator->validate(key, value);
            }
        } else {
            attributes[key] = std::make_shared<value_t>();
        }
        attributes[key]->value = value;
        copy = attributes[key]->copyListeners();
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

    T* value = boost::get<T>(&iter->second->value);

    if (value == nullptr) {
        std::cerr << iter->second->value << std::endl;
        throw std::invalid_argument(
                "Configuration::getParameter: key \"" + key + "\" (which is " +
                std::to_string(iter->second->value.which()) + ") is not " +
                type_name<T>::value);
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
        line << std::boolalpha << attribute.first.c_str() << " = ["
             << attribute.second->value << "]" << std::endl;
        out << line.str();
    }

    return out;
}

void Configuration::addAlias(const std::string& key, const std::string& alias) {
    attributes[alias] = attributes[key];
}

void Configuration::addValueChangedListener(const std::string &key,
                                            ValueChangedListener *val) {
    LockHolder lh(mutex);
    if (attributes.find(key) != attributes.end()) {
        attributes[key]->changeListener.emplace_back(val);
    }
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

void Configuration::addStats(ADD_STAT add_stat, const void *c) const {
    LockHolder lh(mutex);
    for (const auto& attribute :  attributes) {
        if (!requirementsMet(*attribute.second)) {
            continue;
        }
        std::stringstream value;
        value << std::boolalpha << attribute.second->value;
        std::stringstream key;
        key << "ep_" << attribute.first;
        std::string k = key.str();
        add_casted_stat(k.c_str(), value.str().data(), add_stat, c);
    }
}

/**
 * Internal container of an engine parameter.
 */
class ConfigItem: public config_item {
public:
    ConfigItem(const char *theKey, config_datatype theDatatype) :
                                                                holder(NULL) {
        key = theKey;
        datatype = theDatatype;
        value.dt_string = &holder;
    }

private:
    char *holder;
};

bool Configuration::parseConfiguration(const char *str,
                                       SERVER_HANDLE_V1* sapi) {
    std::vector<std::unique_ptr<ConfigItem> > config;

    for (const auto& attribute : attributes) {
        config.push_back(std::make_unique<ConfigItem>(
                attribute.first.c_str(),
                config_datatype(attribute.second->value.which())));
    }

    // And add support for config files...
    config.push_back(std::make_unique<ConfigItem>("config_file", DT_CONFIGFILE));

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

Configuration::~Configuration() = default;
