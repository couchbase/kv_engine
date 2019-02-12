# Collections

## Collections::VB::Manifest

Each vbucket owns a VB::Manifest object and this object stores

* a map of collectionID -> collection meta-data
* a set of scopes

Only scopes and collection's which are available for CRUD operations are stored
in the VB::Manifest.

The VB::Manifest is able to update itself when passed a Manifest object (which
represents the current state as set by the cluster manager). The VB::Manifest
will

* add collections to the map
* drop collections from the map
* add scopes to the set
* drop scopes from the set

Each of these operations triggers the generation of a special mutation to the
VB, a SystemEvent. The job of the system-event is to transmit the collection
state change through the checkpoint and to the checkpoint consumers -
persistence and DCP.

This document contains some brief diagrams to help aid the understanding of
important state changes within the collection's code and the impact those
state changes have on persistence and DCP.

## SystemEvents

The SystemEvents are represented by the Item object. We weave SystemEvents into
the Checkpoint allowing DCP and the Flusher to see the event and then respond to
it. A SystemEvent Item can also be marked as deleted which collections utilises
for marking the drop of a collection or scope

A SystemEvent is a special case of Item and is identified primarily by the
operation member being set to "queue_op::system_event". The type (collection or
scope event) of is stored in the flags field.

The SystemEvent's also have a special name-space prefix in the key to ensure
they don't collide with user data, this prefix is the namespace 1 (System
namespace, or System collection).

### SystemEvent Key

A SystemEvent key is structured so that it does not conflict with real data or
other system events. The following diagram shows the key's components.

```
                ┌────┬────────────┬────┐
              ┌▶│ \1 │_collection:│LEgq│◀──┐
              │ └────┴─────▲──────┴────┘   │
              │            │               │
┌───────────┐ │     ┌──────┴──────┐    ┌───┴───────────┐
│ namespace │ │     │    event    │    │  affected ID  │
│  prefix   │─┘     │   family    │    └───────────────┘
└───────────┘       │             │
                    └─────────────┘
```

Consider a collection with a integer value of 0x4c456771, for this document this
value is deliberately in the printable ascii range, and would be seen as 'LEqq'
when interpreted as a ascii string:

* Creating the collection generates the following Item.
  * event = 0 `SystemEvent::Collection`, key = `\1_collection:LEgq`, deleted = false
* Logically deleting the collection generates the following Item.
  * event = 0 `SystemEvent::Collection`, key = `\1_collection:LEgq`, deleted = true

SystemEvent's affecting Scopes are similar.

Consider a scope with a integer value of 0x4c456771

* Creating the scope generates the following Item.
  * event = 0 `SystemEvent::Scope`, key = `\1_scope:LEgq`, deleted = false
* Logically deleting the scope generates the following Item.
  * event = 0 `SystemEvent::Scope`, key = `\1_scope:LEgq`, deleted = true

### SystemEvent flushing actions

SystemEvents are treated differently by the flusher.

* `Collection`
  * Sets or Deletes a document called `$collection:LEgq` with a value that at least contains the UID
  * Updates the `_local/collections_manifest` (A JSON copy of the VB::Manifest)

## KVStore and SystemEvents

KVStore provides two methods that are for storing SystemEvents, setSystemEvent
and delSystemEvent. These methods inspect the Item being stored and extract data
that is required for maintaining persisted collection meta-data.

* The most recent manifest unique identifier which changed the state.
* The set of collections available for CRUD operations.
* The set of scopes available.
* The set of collections that are dropped but still may have data in storage.

KVStore maintains the following persisted data which can be retrieved by calling
* `getCollectionsManifest`
* `getDroppedCollections`

### Dropped Collections
The management of dropped collections is owned by the KVStore implementation.
For example couch-kvstore has to use compaction to remove the keys of dropped
collections so it does maintain a list of collections. Other kvstore's may have
the ability to drop a collection atomically as part of the commit, those
implementations don't need to maintain a list of dropped collections and can
return empty via `getDroppedCollections`.

## Dropping a collection (couch-kvstore)

Dropping a collection is integrated into the compaction methods, as the
tombstone checker is invoked on each key, the key is also checked against the
list of dropped collections.

Note that only the keys of the collection are actually dropped, the system event
representing the drop of the collection remains until it is actually tombstone
purged.

