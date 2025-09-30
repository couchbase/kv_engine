# audit daemon README

17 January 2018

Daniel Owen, owend@couchbase.com

Version 2.0

## Overview

The design of auditd is such that the daemon and format of audit
events are de-coupled. This allows module owners to define new events
without requiring changes to auditd.

The de-coupling is achieved by using JSON formatted descriptor files.
A system wide module descriptor file defines the modules.  Each module
then has its own event descriptor file that defines all the audit
events for that particular module.

As part of the building of memcached, a build program (called
auditevent_generator) takes the module descriptor file as input and
uses it to pull-in all the individual event descriptor files.  The
build program validates their JSON content and then combines them to
produce a single *audit_events.json* file.  This file is then
installed on the customer's system. (see
/etc/security/audit_events.json)

## The Module Descriptor File

The module descriptor file is a system wide JSON structure used to
specify each of the modules that will generate audit events.  Each
module has a name, start event id (which must be a multiple of 0x1000)
and path to a file that defines the event descriptions for that
module.

The `modules` attribute is an array of objects where each object
defines a single object module.

* `name` - The name of the module (this must be unique)

* `startid` - startid specifies the first identifier used by the
  module. The module may only define audit events within the range
  of the `[ startid, startid+4096 >`

* `file` - The file (relative to the source root) containing the
  modules various audit event descriptors

* `generate_macros` - Set to true to generate #define's for the
  event ids. This is useful for using the events in C++ code
  without having to hard code the event id's.

* `configuration` - An array containing the build configurations
  the module should be included in (KV_CV, EE, CE, Columnar)

In the example below, a single module is defined (auditd).  It
creates audit events from 0x1000 and the events definitions for this
module are found in `kv_engine/auditd/etc/auditd_descriptor.json`.

    {
       "modules" : [
         {
           "auditd" : {
             "startid" : 4096,
             "file": "kv_engine/auditd/etc/auditd_descriptor.json",
             "configuration" : ["KV_CV", "EE", "CE", "Columnar"]
           }
         }
       ]
    }

## The Per Module Events Descriptor File

An events descriptor JSON structure contains a definition of the
events for the module, and the following attributes must be present:

 * version (number) - the version of the auditd format to be used.
           Only version 2 is supported
 * module (string) - the name of the module. Must match the name used
          in the module descriptor file.
 * events (array) - an array of objects where each object defines a single
          audit event.

Example:

    {
      "version": 2,
      "module": "auditd",
      "events": [
        {
          "id": 4096,
          "name": "configured audit daemon",
          "description": "loaded configuration file for audit daemon",
          "sync": false,
          "enabled": true,
          "filtering_permitted": false,
          "mandatory_fields": {
            "timestamp": "",
            "real_userid": {
              "domain": "",
              "user": ""
            },
            "hostname": "",
            "version": 1,
            "auditd_enabled": true,
            "rotate_interval": 1,
            "log_path": "",
            "descriptors_path": ""
          },
          "optional_fields": {}
        }
      ]
    }

The module defines 1 event; for each event the following fields may be specified:

* id (number, mandatory) - the event id; it must be >= startid and <= startid + 0xFFF
* name (string, mandatory) - short textual name of the event
* description (string, mandatory) - longer name / description of the event
* sync (bool, optional) - whether the event is synchronous.
  Currently only async events are supported (default if not specified)
* enabled (bool, optional) - whether the event should be enabled by default.
* filtering_permitted (bool, optional) - whether the event can be filtered or not.
  If the attribute is not defined then it is defaulted that the event
  cannot be filtered.
* mandatory_fields (object, mandatory) - field(s) required for a valid instance of the
  event.
* optional_fields (object, optional) - optional field(s) valid in an instance of the event.
  Note: it is valid to have an empty optional_fields, i.e. {}.

### Defining the format for mandatory and optional fields

The format of mandatory and optional fields are defined to allow
validation of the events submitted to auditd.  Note: Only simple validation
is implemented (that all the mandatory fields are present).

Field types can be any valid JSON type.  The type is specified by
providing the following default values:

* number: 1
* string: ""
* boolean: true
* array: []
* object: {}

The example JSON structure below shows the definition for the 4
pre-defined mandatory fields; *timestamp*, *real_userid*, *remote*, *local*,
and the 2 pre-defined optional fields; *sessionID* and *effective_userid*.

    {"version" : 2,
    "module" : "example",
    "events" : [
                {  "id" : 8192,
                   "name" : "example event",
                   "description" : "this is the full description of the example event",
                   "mandatory_fields" : {
                                         "timestamp" : "",
                                         "real_userid" : {"domain" : "", "user" : ""},
                                         "remote" : {"ip" : "", "port" : 1},
                                         "local": {"ip" : "", "port" : 1}
                                        },
                   "optional_fields" : {
                                        "sessionid" : "",
                                        "effective_userid" : {"domain" : "", "user" : ""}
                                       }
                }
               ]
    }

#### Pre-defined Fields

* `timestamp` - Contains the date and time of the event, in ISO 8601 format.
  Uses local time with timezone offset (from UTC) in hours and minutes.
  Records microsecond granularity using 3 digits after decimal point.
* `real_userid` and `effective_userid` - comprises a "domain", which states
  where the user is defined: "local" (in Couchbase) or "external" (outside
  Couchbase; LDAP etc). It then contains the user string. Example:
  ` {"domain":"local","user":"@ns_server"}`
* `remote` and `local` - An object containing two fields `ip` and `port`

#### Pre-defined Optional Fields

* sessionid - Used to correlate activities i.e. user logs-in to Admin UI,
  sets up XDCR, tweaks a memory value etc.
* remote - the IP address of the remote agent who is requesting this action.
  Note there are sometimes more than one logical remote IP address, e.g. a
  client runs a query which scans a 2i index. In 2i the remote IP could be
  the true client or the query process. In this case we record the IP address
  where the query process is running and use the session ID (see above) to
  resolve to the ultimate client.
* effective_userid - Can be best explained through an example; query node
  connects to an indexing node on behalf of an SDK client. real_userid is
  "_admin" (query auth's as the internal admin); effective user ID is what
  ever the client's ID is.

#### Duration

* The query team include durations in some audit events.  We want to
  ensure that events across different modules use the same format. The
  format follows that used by golang's Duration function.  See
  https://golang.org/src/time/time.go?s=22752:22785#L659

  This is a string representing the duration in the form "72h3m0.5s".
  Leading zero units are omitted.  Durations less than one second
  format use a smaller unit (milli-, micro-, or nanoseconds) to ensure
  that the leading digit is non-zero. The zero duration format is 0s.
  Example outputs are as follows:

  - 4440h0m0.000000001s (precision of 9)
  - 81.678396ms (precision of 6)
  - 392.391µs (precision of 3)

  As a duration is just a string a field such as elapsed_time can be
  defined in an event as `"elapsed_time: ""`.

## How to define events

First an entry for the module must be made in the system wide module
descriptor file; see above for the format of this file.  An entry
requires the definition of a path to the appropriate events descriptor
file.  Therefore the second requirement is to ensure that such a file
exists for the module.

The per module events descriptor file should be within the module and
remains under the control of the module owner.  They are responsible
for defining the audit events it contains.  New events are defined by
adding a JSON object to the events lists in this file.  It is
important to use a unique id and the id is within the range specified
in the module descriptor.  Once all the fields for an event have been
specified it can now be used by the module.

## How to use events

Events are sent to auditd using the memcached protocol through a new
command AUDIT_PUT.  The command request header is defined as follows.
The request header contains an extra element of 4 bytes stating the ID
of the audit-event.

     typedef union {
       struct {
            protocol_binary_request_header header;
            struct {
                uint32_t id;
            } body;
        } message;
        uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
    } protocol_binary_request_audit_put;

    typedef protocol_binary_response_no_extras protocol_binary_response_audit_put;

The payload consists of a JSON object containing all the mandatory and
optional fields that have been defined for the event.  If the event
has only the 2 standard mandatory fields then an example payload would
be as follows:

     {
      "timestamp" : "2014-11-05T13:15:30Z",
      "real_userid" : {"domain": "local", "user" : "@ns_server"}
     }

If the event contained also contained the additional 3 pre-defined
optional fields then an example payload would be as follows:

    {
     "timestamp" : "2014-11-05T13:15:30Z",
     "real_userid" : {"domain": "local", "user" : "@ns_server"},
     "sessionID" : "SID:ANON:www.w3.org:j6oAOxCWZh/CD723LGeXlf-01:34",
     "remote" : {"ip": "127.0.0.1", "port" : 11210"},
     "effective_userid" : {"domain": "external", "user" : "joeblogs"},
    }


## Configuring auditd

auditd is configured using a configutation file that is loaded when
the daemon is started.  It can also be reloaded on-demand using the
memcached protocol through a new command
PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD.

The configuration file is a JSON structured document that comprises of
the following fields:

* version - states which format of the auditd to use.  Currently only "2" is
  valid
* daemon enabled - boolean stating whether the daemon should be running.
* rotate interval - number of minutes between log file rotation. (Default is
  one day.  Minimum is 15 minutes)
* rotate_size - number of bytes written to the file before rotating to a new
  file
* prune_age - (optional field) Prune all audit log files older than the
  specified value (in seconds). (set to 0 (or remove) to disable the
  functionality).
* buffered - should buffered file IO be used or not
* sync - list of event ids containing those events that are synchronous.
  Synchronous events are not supported in Sherlock and so this should be the
  empty list.
* uuid - identifies which auditd configuration is being used. The value is
  provided by ns_server
* disabled_userids - a list of userids. Each entry corresponds to a userid
  that we want to be filtered out. Note: The list can be empty.
* enabled_userids - a list of userids. Each entry corresponds to a userid
  that we want to be explicitly enabled (all other users are disabled).
  This is *mutually exclusive* with `disabled_userid`. Each user is specified
  as an object with two fields "domain" and "user" where domain may be one
  of `local`, `external` or `unknown`.
* filtering_enabled - boolean stating whether filtering is enabled. This
  configuration overrides all other filtering options. i.e. if set to false,
  then regardless of other filter settings, no filtering will be performed.
* event_states - map of eventids to states (either enabled or disabled).
  This configuration is used to override the "enabled" attribute of an
   event defined in its module definition. The map is optional and if
    omitted the configuration will still be parsed correctly.

An example verison 2 configuration is presented below.

       {
        "version":      2,
        "uuid":         "uuid_string_provided_by_ns_server"
        "auditd_enabled":       true,
        "rotate_interval":      1440,
        "rotate_size":          20971520,
        "buffered":             true,
        "log_path": "/var/lib/couchbase/logs",
        "event_states" : {"1234" : "enabled", "5678" : "disabled"}
        "disabled_userids": [{"domain" : "local, "user" : "joeblogs"}],
        "sync": []
       }

## Filtering Support

With the introduction of Version 2 of the auditd configuration filtering by
user is supported.  For filtering to work the filtering_enabled setting in the
auditd configuration file must be set to true.

In addition, the disabled_userids list must contain the userids that are to be
filtered out.  The domain must match the "domain" component from a real_userid
or effective_userid, and the user must match the "user" component.
For example given the following:

        "real_userid" : {"domain": "local", "user" : "joeblogs"}
        "effective_userid" : {"domain": "external", "user" : "joeblogs"},

If it was decided to filter out the events from {"domain" : "local",
"user" : "joeblogs"} then the disabled_userids list would be as follows:

        "disabled_userids": [{"domain" : "local", "user" : "joeblogs"}]

Finally, an event will only be filtered if its "filtering_permitted" attribute
is set to true in the definition of the event.  If the event does not contain
the "filtering_permitted" attribute, or it is set to false then the event will
not be filtered, even if it was generated by a user that is in the
disabled_userids list.

### Filtering At Source Optimisation

Client modules can perform filtering at source to avoid the need to send audit
events to the audit daemon only for them to be dropped.

If this is implemented, the module doing the filtering must generate an audit
event when they start (and stop) filtering events, together with the uuid of the
configuration used.  The motivation behind this is that we need to be able to
determine if an audit event was expected to be dropped by the frontend or if it
is missing in the audit trail.

As an example, lets suppose we decide to filter on userA and then at a later
time we decide to clear the filter so that userA’s events are no longer filtered
out.  Unless we record in the audit log when we made the change to the filter we
don’t know if events have been filtered by the audit daemon or whether userA
simply did not generate certain events.

Therefore if filtering is performed at source, there is requirement for the
module to define a {MODULE_NAME} configuration event.  For example if N1QL
implements event filtering at the source, it will need to define a
"N1QL configuration" event.  This event must contain a copy of the uuid
identifying which version of the configuration file it is currently using.  An
example of a configuration event is given below:

       {
        "id" : 28689,
        "name" : "N1QL configuration",
        "description" : "States that N1QL is using audit configuration with specified uuid",
        "sync" : false,
        "enabled" : true,
        "filtering_permitted" : false,
        "mandatory_fields" : {
                              "timestamp" : "",
                              "real_userid" : {"domain" : "", "user" : ""},
                              "uuid" : ""
                             },
        "optional_fields" : {}
       }

Note: The filtering_permitted setting for this event must be set to false.

It is the reponsibility of the client module to generate a configuration event
every time it picks up a new auditd configuration file (from ns_server).  This
ensures that the latest auditd configuration version is recorded in the audit
log when a module is filtering at source.
