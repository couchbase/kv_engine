/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc.
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
#ifndef COMMAND_IDS_H
#define COMMAND_IDS_H 1

#define CMD_STOP_PERSISTENCE  0x80
#define CMD_START_PERSISTENCE 0x81
#define CMD_SET_FLUSH_PARAM   0x82

#define CMD_START_REPLICATION 0x90
#define CMD_STOP_REPLICATION  0x91
#define CMD_SET_TAP_PARAM     0x92
#define CMD_EVICT_KEY         0x93
#define CMD_GET_LOCKED        0x94
#define CMD_UNLOCK_KEY        0x95

#define CMD_SYNC              0x96

/*
 * IDs for the events of the SYNC command.
 */
#define SYNC_PERSISTED_EVENT 1
#define SYNC_MODIFIED_EVENT  2
#define SYNC_DELETED_EVENT   3
#define SYNC_INVALID_KEY     4
#define SYNC_INVALID_CAS     5

#endif /* COMMAND_IDS_H */
