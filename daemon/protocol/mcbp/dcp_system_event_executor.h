/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "../../memcached.h"

/**
 * Implementation of the callback method called from the underlying engine
 * when we want to create a DCP_SYSTEM_EVENT packet.
 *
 * @param cookie The cookie provided from the frontend representing the
 *               connection.
 * @param opaque The opaque value the other end requested to be in the packets
 * @param vbucket The vbucket the object resides in
 * @param event The engine's system event ID
 * @param bySeqno The by_seqno value the data is stored in the vbucket
 * @param key buffer containing the event's key
 * @param eventData buffer containing event data which is shipped in the body
 * @return ENGINE_SUCCESS or ENGINE_E2BIG if there's no space in the send buffer
 */
ENGINE_ERROR_CODE dcp_message_system_event(gsl::not_null<const void*> cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket,
                                           mcbp::systemevent::id event,
                                           uint64_t bySeqno,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData);

/**
 * Implementation of the method responsible for handle the incoming
 * DCP_SYSTEM_EVENT packet.
 */
void dcp_system_event_executor(Cookie& cookie);
