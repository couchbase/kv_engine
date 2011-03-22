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

/**
 * Start restoring a <b>single</b> incremental backup file specified in the
 * key field of the packet.
 * The server will return the following error codes:
 * <ul>
 *  <li>PROTOCOL_BINARY_RESPONSE_SUCCESS if the restore process is started</li>
 *  <li>PROTOCOL_BINARY_RESPONSE_KEY_ENOENT if the backup file couldn't be found</li>
 *  <li>PROTOCOL_BINARY_RESPONSE_AUTH_ERROR if the user isn't admin (not implemented)</li>
 *  <li>PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED if the server isn't in restore mode</li>
 *  <li>PROTOCOL_BINARY_RESPONSE_EINTERNAL with a description what went wrong</li>
 * </ul>
 *
 */
#define CMD_RESTORE_FILE 0x83

/**
 * Try to abort the current restore as soon as possible. The server
 * <em>may</em> want to continue to process an unknown number of elements
 * before aborting (or even complete the full restore). The server will
 * <b>always</b> return with PROTOCOL_BINARY_RESPONSE_SUCCESS even if the
 * server isn't running in restore mode without any restore jobs running.
 */
#define CMD_RESTORE_ABORT 0x84

/**
 * Notify the server that we're done restoring data, so it may transition
 * from restore mode to fully operating mode.
 * The server always returns PROTOCOL_BINARY_RESPONSE_SUCCESS
 */
#define CMD_RESTORE_COMPLETE 0x85

/*
 * IDs for the events of the SYNC command.
 */
#define SYNC_PERSISTED_EVENT    1
#define SYNC_MODIFIED_EVENT     2
#define SYNC_DELETED_EVENT      3
#define SYNC_REPLICATED_EVENT   4
#define SYNC_INVALID_KEY        5
#define SYNC_INVALID_CAS        6

#endif /* COMMAND_IDS_H */
