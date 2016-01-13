/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015 Couchbase, Inc
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

#include <memcached/engine.h>

#include <atomic>
#include <relaxed_atomic.h>

/**
 * An enumeration with constants for the various protocols supported
 * on the server.
 */
enum class Protocol : uint8_t {
    /** The memcached binary protocol used in Couchbase < 4.0 */
    Memcached,
    /** The (new and upcomming) Greenstack protocol */
    Greenstack
};

const char* to_string(const Protocol& protocol);


struct interface {
    const char *host;
    struct {
        const char *key;
        const char *cert;
    } ssl;
    int maxconn;
    int backlog;
    in_port_t port;
    bool ipv6;
    bool ipv4;
    bool tcp_nodelay;
    Protocol protocol;
};

/* pair of shared object name and config for an extension to be loaded. */
struct extension_settings {
    const char* soname;
    const char* config;
};

/* What information should breakpad minidumps contain? */
typedef enum {
    CONTENT_DEFAULT // Default content (threads+stack+env+arguments) */
} breakpad_content_t;

/* Settings for Breakpad crach catcher. */
typedef struct {
    bool enabled;
    const char* minidump_dir;
    breakpad_content_t content;
} breakpad_settings_t;

/* When adding a setting, be sure to update process_stat_settings */
/**
 * Globally accessible settings as derived from the commandline / JSON config
 * file.
 */
struct settings {

    /*************************************************************************
     * These settings are directly exposed via the config file / cmd line
     * options:
     */
    const char *admin;      /* admin username */
    bool disable_admin;     /* true if admin disabled. */
    int num_threads;        /* number of worker (without dispatcher) libevent
                               threads to run */
    struct interface *interfaces; /* array of interface settings we are
                                     listening on */
    int num_interfaces;     /* size of {interfaces} */
    /* array of extensions and their settings to be loaded. */
    struct extension_settings *pending_extensions;
    int num_pending_extensions; /* size of above array. */
    const char *audit_file; /* The file containing audit configuration */
    const char *rbac_file; /* The file containing RBAC information */
    bool rbac_privilege_debug; /* see manpage */
    bool require_sasl;      /* require SASL auth */
    std::atomic<int> verbose;            /* level of versosity to log at. */
    /** The number of seconds a client may be idle before it is disconnected */
    Couchbase::RelaxedAtomic<size_t> connection_idle_time;
    unsigned int bio_drain_buffer_sz; /* size of the SSL bio buffers */
    bool datatype;          /* is datatype support enabled? */
    const char *root; /* The root directory of the installation */

    /* Maximum number of io events to process based on the priority of the
       connection */
    int reqs_per_event_high_priority;
    int reqs_per_event_med_priority;
    int reqs_per_event_low_priority;
    int default_reqs_per_event;

    breakpad_settings_t breakpad; /* Breakpad crash catcher settings */
    /**
     * To prevent us from reading (and allocating) an insane amount of
     * data off the network we'll ignore (and disconnect clients) that
     * tries to send packets bigger than this max_packet_size. See
     * the man page for more information.
     */
    uint32_t max_packet_size;
    bool require_init; /* Require init message from ns_server */

    const char *ssl_cipher_list; /* The SSL cipher list to use */

    /**
     * The number of topkeys to track
     */
    int topkeys_size;


    /**
     * Listen on stdin (reply to stdout)
     */
    bool stdin_listen;

    /**
     * When *any* connection closes, terminate the process.
     * Intended for afl-fuzz runs.
     */
    bool exit_on_connection_close;

    /**
     * The available sasl mechanism list
     */
    const char *sasl_mechanisms;

    /* flags for each of the above config options, indicating if they were
     * specified in a parsed config file.
     */
    struct {
        bool admin;
        bool threads;
        bool interfaces;
        bool extensions;
        bool audit;
        bool rbac;
        bool rbac_privilege_debug;
        bool require_sasl;
        bool reqs_per_event_high_priority;
        bool reqs_per_event_med_priority;
        bool reqs_per_event_low_priority;
        bool default_reqs_per_event;
        bool verbose;
        bool connection_idle_time;
        bool bio_drain_buffer_sz;
        bool datatype;
        bool root;
        bool breakpad;
        bool max_packet_size;
        bool require_init;
        bool ssl_cipher_list;
        bool topkeys_size;
        bool stdin_listen;
        bool exit_on_connection_close;
        bool sasl_mechanisms;
    } has;
    /*************************************************************************
     * These settings are not exposed to the user, and are either derived from
     * the above, or not directly configurable:
     */
    int maxconns;           /* Total number of permitted connections. Derived
                               from sum of all individual interfaces */
    bool sasl;              /* SASL on/off */
    int topkeys;            /* Number of top keys to track */

    /* linked lists of all loaded extensions */
    struct {
        EXTENSION_DAEMON_DESCRIPTOR *daemons;
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *binary;
    } extensions;

    const char *config;      /* The configuration specified by -C (json) */
    /* @todo fix config of this! (not dynamic as of now), I guess
     * @todo I should just move into a C++ file and stick it in
     * @todo std::list
     */
    int max_buckets; /* the maximum number of buckets */
};

extern struct settings settings;
