/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "priority.hh"

const Priority Priority::BgFetcherPriority("bg_fetcher_priority", 0);
const Priority Priority::TapBgFetcherPriority("bg_fetcher_priority", 1);
const Priority Priority::SetVBucketPriority("set_vbucket_priority", 2);
const Priority Priority::FlusherPriority("flusher_priority", 5);
const Priority Priority::ItemPagerPriority("item_pager_priority", 7);
const Priority Priority::VBucketDeletionPriority("vbucket_deletion_priority", 9);
