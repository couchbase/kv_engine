/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
 * when we want to create a DCP_MUTATION packet.
 *
 * @param void_cookie The cookie provided from the frontend representing the
 *                    connection
 * @param opaque The opaque value the other end requested to be in the packets
 * @param it The actual item to transfer
 * @param vbucket The vbucket the object resides in
 * @param by_seqno The by_seqno value the data is stored in the vbucket
 * @param rev_seqno The revision sequence number
 * @param lock_time The lock time to fill into the packet
 * @param meta The encoded extended meta information as provided by the
 *             underlying engine. If the mutation contains xattrs we'll
 *             encode the size of the xattrs into the meta information
 * @param nmeta The size of the extended meta information
 * @param nru The nru bits to fill in for the object
 * @return ENGINE_SUCCESS if the message was successfully inserted into
 *                        the stream, the standard engine return values
 *                        if a failure happens.
 */
ENGINE_ERROR_CODE dcp_message_mutation(const void* void_cookie,
                                       uint32_t opaque,
                                       item* it,
                                       uint16_t vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t lock_time,
                                       const void* meta,
                                       uint16_t nmeta,
                                       uint8_t nru);

/**
 * Implementation of the method responsible for handle the incoming
 * DCP_MUTATION packet.
 *
 * @param c the connection the packet arrived on
 * @param packet the full DCP_MUTATION packet
 */
void dcp_mutation_executor(McbpConnection* c, void* packet);
