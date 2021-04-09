/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "connhandler.h"

#include <statistics/cbstat_collector.h>
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
