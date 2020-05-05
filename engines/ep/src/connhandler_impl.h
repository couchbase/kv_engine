/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
#pragma once

#include "connhandler.h"
#include "statistics/collector.h"

#include <sstream>

/*
 * Contains implementation details of ConnHandler which arn't part of the
 * (public) interface but /are/ required inline by ConnHandler and its
 * subclasses, or if called by clients.
 *
 * For example; the definitions of template functions which we don't want
 * to define inline to avoid #include pollution.
 */

template <typename T>
void ConnHandler::addStat(const char* nm,
                          const T& val,
                          const AddStatFn& add_stat,
                          const void* c) const {
    std::stringstream tap;
    tap << name << ":" << nm;
    std::string n = tap.str();
    add_casted_stat(n.data(), val, add_stat, c);
}
