# audit daemon README

8 January 2015

Daniel Owen, owend@couchbase.com

Version 1.0

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

In the example below, a single module is defined (auditd).  It
creates audit events from 0x1000 and the events definitions for this
module are found in "memcached/auditd/etc/auditd_descriptor.json".
Each module can specify a maximum of 4095 events (i.e. for the
auditd module this is from 0x1000 to 0x1FFF).


    {
     "modules" : [
                  {
                   "auditd" : {
                                 "startid" : 4096,
                                 "file" : "memcached/auditd/etc/auditd_descriptor.json"
                                }
                  }
                 ]
    }

## The Per Module Events Descriptor File

An events descriptor JSON structure contains a definition of the
events for the module.  The JSON structure contains a version field;
stating the auditd format version used.  Currently "1" is the only
valid value.  The second field contains the module name.  This needs
to match the name used in the module descriptor file.  The third field
is a list of all the events that are defined for the module.  An
example event descriptor file for the auditd module is given below.

    {
     "version" : 1,
     "module" : "auditd",
     "events" : [
                 {  "id" : 4096,
                    "name" : "configured audit daemon",
                    "description" : "loaded configuration file for audit daemon",
                    "sync" : false,
                    "enabled" : true,
                    "mandatory_fields" : {
                                          "timestamp" : "",
                                          "real_userid" : {"source" : "", "user" : ""},
                                          "hostname" : "",
                                          "version" : 1,
                                          "auditd_enabled" : true,
                                          "rotate_interval" : 1,
                                          "log_path" : "",
                                          "descriptors_path" : ""
                                         },
                    "optional_fields" : {}
                 },

                 {  "id" : 4097,
                    "name" : "enabled audit daemon",
                    "description" : "The audit daemon is now enabled",
                    "sync" : false,
                    "enabled" : true,
                    "mandatory_fields" : {
                                          "timestamp" : "",
                                          "real_userid" : {"source" : "", "user" : ""}
                                         },
                    "optional_fields" : {}
                 },

                 {  "id" : 4098,
                    "name" : "disabled audit daemon",
                    "description" : "The audit daemon is now disabled",
                    "sync" : false,
                    "enabled" : true,
                    "mandatory_fields" : {
                                          "timestamp" : "",
                                          "real_userid" : {"source" : "", "user" : ""}
                                         },
                    "optional_fields" : {}
                 },

                 {  "id" : 4099,
                    "name" : "shutting down audit daemon",
                    "description" : "The audit daemon is being shutdown",
                    "sync" : false,
                    "enabled" : true,
                    "mandatory_fields" : {
                                          "timestamp" : "",
                                          "real_userid" : {"source" : "", "user" : ""}
                                         },
                    "optional_fields" : {}
                 }
                ]
}


The module defines 4 events; for each event 7 fields must be specified:

* id (number) - the event id; it must be >= startid and <= startid + 0xFFF
* name (string) - short textual name of the event
* description (string) - longer name / description of the event
* sync (bool) - whether the event is synchronous.  Currently only async events are supported
* enabled (bool) - whether the event should be outputted in the audit log.  This feature can be used to depreciate an event, if required.
* mandatory_fields (object) - field(s) required for a valid instance of the event.  In version 1 of the audit format *timestamp* and *real_userid* fields are required (see below).  Other bespoke fields can be added.
*  optional_fields - optional field(s) valid in an instance of the event.  Three standard optional_fields are defined in version 1; *sessionID*, *remote* and *effective_userid*.  However additional bespoke fields can be added, if required.  Note: it is valid to have an empty optional_fields, i.e. {}.

### Defining the format for mandatory and optional fields

The format of mandatory and optional fields are defined to allow
validation of the events submitted to auditd.  Note: for Sherlock
validation will not be implemented.

Field types can be any valid JSON type.  The type is specified by
providing the following default values:

* number: 1
* string: ""
* boolean: true
* array: []
* object: {}

The example JSON structure below shows the definition for the 2
pre-defined mandatory fields; *timestamp* and *real_userid*, and the 3
pre-defined optional fields; *sessionID*, *remote* and
*effective_userid*.

    {"version" : 1,
    "module" : "example",
    "events" : [
                {  "id" : 8192,
                   "name" : "example event",
                   "description" : "this is the full description of the example event",
                   "sync" : false,
                   "enabled" : true,
                   "mandatory_fields" : {
                                         "timestamp" : "",
                                         "real_userid" : {"source" : "", "user" : ""}
                                        },
                   "optional_fields" : {
                                        "sessionid" : ""
                                        "remote" : {"ip" : "", "port" : 1}
                                        "effective_userid" : {"source" : "", "user" : ""}
                                       }
                }
               ]
    }

#### Pre-defined Mandatory Fields

* timestamp - Contains the date and time of the event, in ISO 8601 format.  Uses local time with timezone offset (from UTC) in hours and minutes.  Records microsecond granularity using 3 digits after decimal point.
* real_userid - comprises of a "source", which states where the user is defined, e.g. internal, ldap or ad.  It then contains the user information, which is recorded differently, depending on the source; and hence is stored in a JSON object.

#### Pre-defined Optional Fields

* sessionid - Used to correlate activities i.e. user logs-in to Admin UI, sets up XDCR, tweaks a memory value etc.
* remote - the IP address of the remote agent who is requesting this action.  Note there are sometimes more than one logical remote IP address, e.g. a client runs a query which scans a 2i index.  In 2i the remote IP could be the true client or the query process.  In this case we record the IP address where the query process is running and use the session ID (see above) to resolve to the ultimate client.
* effective_userid - Can be best explained through an example; query node connects to an indexing node on behalf of an SDK client. real_userid is "_admin" (query auth's as the internal admin); effective user ID is what ever the client's ID is.

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
      "real_userid" : {"source": "internal", "user" : "_admin"}
     }

If the event contained also contained the additional 3 pre-defined
optional fields then an example payload would be as follows:

    {
     "timestamp" : "2014-11-05T13:15:30Z",
     "real_userid" : {"source": "internal", "user" : "_admin"},
     "sessionID" : "SID:ANON:www.w3.org:j6oAOxCWZh/CD723LGeXlf-01:34",
     "remote" : {"ip": "127.0.0.1", "port" : 11210"},
     "effective_userid" : {"source": "ldap", "user" : "cn=John Doe, dc=example, dc=com"},
    }


## Configuring auditd

auditd is configured using a configutation file that is loaded when
the daemon is started.  It can also be reloaded on-demand using the
memcached protocol through a new command
PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD.

The configuration file is a JSON structured document that comprises of
the following fields:

* version - states which format of the auditd to use.  Currently only "1" is valid.
* daemon enabled - boolean stating whether the daemon should be running.
* rotate interval - number of minutes between log file rotation.  (Default is one day.  Minimum is 15 minutes)
* rotate_size - number of bytes written to the file before rotating to a new file
* buffered - should buffered file IO be used or not
* disabled - list of event ids (numbers) containing those events that are NOT to be outputted to the audit log.
* sync - list of event ids containing those events that are synchronous.  Synchronous events are not supported in Sherlock and so this should be the empty list.

An example configuration is presented below.

       {
        "version":      1,
        "daemon_enabled":       true,
        "rotate_interval":      1440,
        "rotate_size":          20971520,
        "buffered":             true,
        "log_path", "/var/lib/couchbase/logs",
        "disabled": [],
        "sync": []
       }
