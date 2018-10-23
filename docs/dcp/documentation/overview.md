# Overview

### Data Synchronization

Data synchronization is an important part of any system that manages data, but its especially important in a distributed database. While a traditional databases rely on durability in order to prevent data loss during failures distributed databases rely on redundency of data by making sure that copies of the data are replicated to multiple servers. This allows a distributed system to prevent against against data loss in the face of node failures, rack failures and regional failures.

The database is also the core of any datacenter deployment and the source of all information. The database however is typically not the only backend system in use and a data synchronization protocol is useful for keeping the data in all systems in the datacenter up to date with respect to the database. This is important both internally and externally to Couchbase. Internally Couchbase uses data synchronization to power its indexing, cross datacenter replication, and backup components. Externally the protocol can be used to move data into third-party systems such as Hadoop or ElasticSearch.

### The Database Change Protocol

The Database Change Protocol (DCP) is the data synchronization protocol used by Couchbase Server. It provides features and concepts that allow the development of rich and interesting features. DCP can be viewed as the heart of Couchbase because it pumps data out to other components in the system so that they can function properly.

DCP was developed from the ground up to quickly and efficiently move massive amounts of data around the cluster and datacenter. It provides guarentees of consistency so that applications that consume data with the DCP protocol can make sure that they are making decisions based on a consistent view of the database. DCP also provides fine grained restartability in order to allow consumers of DCP to resume from exactly where they left off no matter how long they have been disconnected. Fine grained restartability is crucial when dealing with massive amounts of data because retrasmitting data that has already been sent may impose high costs. Finally DCP provides high throughput, low latency data transfer to allow applications to get the lastest data change as quickly as possible.

### Intended Readers

The documentation contained on this website is written mainly for those interested in learning the in depth details of how the DCP protocol works in Couchbase or for developers who wish to build their own DCP Consumers to connect their third-party components to Couchbase.

The documentation is split into five sections:

* **Architecture:** Describes the details of the server implementation and provides examples of how different DCP process work. This section is meant for engineers working on Couchbase Server or those who are developing SDKs.

* **Developing Clients:** Describes the best practices for DCP Consumer impementations, how to start projects, and how to add more advanced features to basic implementations. This section is meant for developers who want to create a DCP client for a third-party implementation. (Note: Check to see if a Couchbase supported SDK exists before writing your own)

* **Monitoring:** This section is mainly for people who want to add monitoring capabilities and describes what the different options are for getting stats about DCP connections from the server.

* **Core Server Architecture:** Describes the implementations for internal existing integrations with Couchbase Server. This section is for Couchbase engineers and those who want to learn more about the inner workings of Couchbase.

* **Future Work:** Describes the future features that the Couchbase team will work on. These features may not yet be scheduled for a particular release.
