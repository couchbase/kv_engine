/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include <cstdlib>
#include <iostream>
#include <map>
#include <string>

#include "common.h"
#include "ep_engine.h"
#include "statsnap.h"

extern "C" {
    static void add_stat(const char *key, const uint16_t klen,
                         const char *val, const uint32_t vlen,
                         const void *cookie) {
        assert(cookie);
        void *cokie = const_cast<void *>(cookie);
        StatSnap *ssnap = static_cast<StatSnap *>(cokie);

        ObjectRegistry::onSwitchThread(ssnap->getEngine());
        std::string k(key, klen);
        std::string v(val, vlen);
        ssnap->getMap()[k] = v;
    }
}

bool StatSnap::getStats() {
    map.clear();
    bool rv = engine->getStats(this, NULL, 0, add_stat) == ENGINE_SUCCESS &&
              engine->getStats(this, "tap", 3, add_stat) == ENGINE_SUCCESS;
    if (rv && engine->isShutdownMode()) {
        map["ep_force_shutdown"] = engine->isForceShutdown() ? "true" : "false";
        std::stringstream ss;
        ss << ep_real_time();
        map["ep_shutdown_time"] = ss.str();
    }
    return rv;
}

bool StatSnap::callback(Dispatcher &d, TaskId &t) {
    if (getStats()) {
        engine->getEpStore()->getRWUnderlying()->snapshotStats(map);
    }
    if (runOnce) {
        return false;
    }
    d.snooze(t, STATSNAP_FREQ);
    return true;
}
