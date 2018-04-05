/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "kvstore_config.h"

#include <string>

class Configuration;

// This class represents the PlasmaKVStore specific configuration.
// PlasmaKVStore uses this in place of the KVStoreConfig base class.
class PlasmaKVStoreConfig : public KVStoreConfig {
public:
    // Initialize the object from the central EPEngine Configuration
    PlasmaKVStoreConfig(Configuration& config, uint16_t shardid);

	uint64_t getPlasmaMemQuota() {
		return plasmaMemQuota;
	}
	bool isPlasmaEnableDirectio() {
		return plasmaEnableDirectio;
	}
	bool isPlasmaKvSeparation() {
		return plasmaKvSeparation;
	}
	int getPlasmaLssCleanThreshold() {
		return plasmaLssCleanThreshold;
	}
	int getPlasmaLssCleanMax() {
		return plasmaLssCleanMax;
	}
	int getPlasmaDeltaChainLen() {
		return plasmaDeltaChainLen;
	}
	int getPlasmaBasePageItems() {
		return plasmaBasePageItems;
	}
	int getPlasmaLssNumSegments() {
		return plasmaLssNumSegments;
	}
	int getPlasmaSyncAt() {
		return plasmaSyncAt;
	}
	bool isPlasmaEnableUpsert() {
		return plasmaEnableUpsert;
	}


private:
	// Plasma Memory Quota
	size_t plasmaMemQuota;

	// Plasma Enable Direct I/O
	bool plasmaEnableDirectio;

	// Plasma Enable Key Value Separation
	bool plasmaKvSeparation;

	// Plasma LSS Clean Fragmentation
	size_t plasmaLssCleanThreshold;

	// Plasma LSS Clean Throtle
	size_t plasmaLssCleanMax;

	// Plasma delta chain len
	size_t plasmaDeltaChainLen;

	// Plasma base page len
	size_t plasmaBasePageItems;

	// Plasma LSS Num Segments
	size_t plasmaLssNumSegments;

	// Plasma Sync at ms
	size_t plasmaSyncAt;

	// Plasma enable upsert
	bool plasmaEnableUpsert;

};
