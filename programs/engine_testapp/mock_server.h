/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_testapp.h>
#include <memcached/rbac.h>

ServerApi* get_mock_server_api();

void init_mock_server();

void mock_time_travel(int by);

void mock_set_pre_link_function(PreLinkFunction function);

void mock_set_privilege_context_revision(uint32_t rev);
uint32_t mock_get_privilege_context_revision();

/// Register the given cookie for notifications via notify_io_complete().
void mock_register_cookie(CookieIface& cookie);

/// Unregister the given cookie for notifications via notify_io_complete().
void mock_unregister_cookie(CookieIface& cookie);

void mock_notify_io_complete(CookieIface& cookie, cb::engine_errc status);

/**
 * Wait for at least one call to notify_io_complete to have been made
 * for the given cookie.
 * Returns immediately if notify_io_complete has already been called,
 * returning the result of that notify_io_complete.
 * If notify_io_complete has not been called, blocks waiting for
 * notify_io_complete to be called (and woken via a condition
 * variable).
 *
 * Requires that the cookie has prevsiously been registered for notifications
 * using mock_register_cookie.
 *
 * Note: Each call to this waits for another notification (and consumes it). To
 * check _if_ the cookie has a notification pending without consuming it,
 * use mock_cookie_notified().
 */
cb::engine_errc mock_waitfor_cookie(CookieIface* cookie);

/**
 * Returns true if the cookie has been notified and has a status pending
 * to read.
 */
bool mock_cookie_notified(CookieIface* cookie);

void mock_set_dcp_disconnect_when_stuck_timeout(std::chrono::seconds timeout);

void mock_set_dcp_disconnect_when_stuck_name_regex(std::string regex);

void mock_set_not_locked_returns_tmpfail(bool value);

void mock_set_dcp_consumer_max_marker_version(double value);

void mock_set_dcp_snapshot_marker_hps_enabled(bool value);

void mock_set_dcp_snapshot_marker_purge_seqno_enabled(bool value);

void mock_set_magma_blind_write_optimisation_enabled(bool enabled);
