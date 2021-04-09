#!/bin/bash

# Copyright 2017-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

set -e
set -x

mc_URL=git://github.com/couchbase/memcached
ep_URL=git://github.com/couchbase/ep-engine

# Start with the memcached repo - the layout /mostly/ matches the final result we want.
git clone $mc_URL .
git checkout master

# Rename the memcached tags to add a '_mc' suffix.
git for-each-ref --shell --format="ref=%(refname:strip=2)" refs/tags/ |
    while read entry
    do
        eval "$entry"
        git tag ${ref}_mc ${ref}
        git tag -d ${ref}
    done

# Add ep-engine as a remote, then check that out and move files to
# where we want them post-merge.
git remote add -f ep_engine $ep_URL
git fetch --tags ep_engine
git checkout -b ep_master ep_engine/master
mkdir -p engines/ep
git mv -k .[a-z]* * engines/ep/
git commit -m "Move ep-engine to engines/ep"

# Rename the ep-engine tags to add a '_ep' suffix.
git for-each-ref --shell --format="ref=%(refname:strip=2)" refs/tags/ |
    while read entry
    do
        eval "$entry"
        if [[ $ref != *_mc ]]; then
            git tag ${ref}_ep ${ref}
            git tag -d ${ref}
        fi
    done

# Flip back to "main" master, and merge ep_master
git checkout master
git merge --allow-unrelated-histories ep_master <<EOF
=== Merge ep_engine/master into kv_engine ===
EOF

# Now apply the patches we need post-merge:
git am <<'EOF'
From 986c378c3d45189fdeea8bdacb3a635cde8a8b94 Mon Sep 17 00:00:00 2001
From: Dave Rigby <daver@couchbase.com>
Date: Mon, 15 May 2017 17:05:35 +0100
Subject: [PATCH] MB-22602: Update auditd file paths for kv_engine

Now memcached & ep-engine are in a unified repo named 'kv_engine',
update paths for auditd to the new location.

Change-Id: Ice4f453af3101194bb0b6b728b2e6585ea474f9b
---
 auditd/etc/kv_commit_validation_module_descriptors.json | 8 ++++----
 auditd/etc/module_descriptors.json                      | 8 ++++----
 2 files changed, 8 insertions(+), 8 deletions(-)

diff --git a/auditd/etc/kv_commit_validation_module_descriptors.json b/auditd/etc/kv_commit_validation_module_descriptors.json
index bc9227b..a441fd7 100644
--- a/auditd/etc/kv_commit_validation_module_descriptors.json
+++ b/auditd/etc/kv_commit_validation_module_descriptors.json
@@ -3,15 +3,15 @@
     {
       "auditd": {
         "startid": 4096,
-        "file": "memcached/auditd/etc/auditd_descriptor.json",
-        "header": "memcached/auditd/auditd_audit_events.h"
+        "file": "kv_engine/auditd/etc/auditd_descriptor.json",
+        "header": "kv_engine/auditd/auditd_audit_events.h"
       }
     },
     {
       "memcached": {
         "startid": 20480,
-        "file": "memcached/etc/memcached_descriptor.json",
-        "header": "memcached/memcached_audit_events.h"
+        "file": "kv_engine/etc/memcached_descriptor.json",
+        "header": "kv_engine/memcached_audit_events.h"
       }
     }
   ]
diff --git a/auditd/etc/module_descriptors.json b/auditd/etc/module_descriptors.json
index a50dc1c..3499707 100644
--- a/auditd/etc/module_descriptors.json
+++ b/auditd/etc/module_descriptors.json
@@ -3,8 +3,8 @@
                {
                 "auditd" :    {
                                "startid" : 4096,
-                               "file" : "memcached/auditd/etc/auditd_descriptor.json",
-                               "header" : "memcached/auditd/auditd_audit_events.h"
+                               "file" : "kv_engine/auditd/etc/auditd_descriptor.json",
+                               "header" : "kv_engine/auditd/auditd_audit_events.h"

                               }
                },
@@ -23,8 +23,8 @@
                {
                 "memcached" : {
                                "startid" : 20480,
-                               "file" : "memcached/etc/memcached_descriptor.json",
-                               "header":"memcached/memcached_audit_events.h"
+                               "file" : "kv_engine/etc/memcached_descriptor.json",
+                               "header":"kv_engine/memcached_audit_events.h"
                               }
                },
                {
--
2.10.0

EOF

git am <<'EOF'
From 47191f54b71f7ab2da158b3709bd8d4e11a24d6c Mon Sep 17 00:00:00 2001
From: Dave Rigby <daver@couchbase.com>
Date: Mon, 15 May 2017 17:01:02 +0100
Subject: [PATCH] MB-22602: Add 'ep' as a subdir to engines/

---
 engines/CMakeLists.txt | 1 +
 1 file changed, 1 insertion(+)

diff --git a/engines/CMakeLists.txt b/engines/CMakeLists.txt
index 523eb2b..48365e5 100644
--- a/engines/CMakeLists.txt
+++ b/engines/CMakeLists.txt
@@ -3,5 +3,6 @@ ADD_SUBDIRECTORY(utilities)

 ADD_SUBDIRECTORY(crash_engine)
 ADD_SUBDIRECTORY(default_engine)
+ADD_SUBDIRECTORY(ep)
 ADD_SUBDIRECTORY(ewouldblock_engine)
 ADD_SUBDIRECTORY(nobucket)
--
2.10.0

EOF

# TLM patch to use the new combined directory:
pushd ../tlm
git am --ignore-whitespace <<'EOF'
From a1d0a9836875a70e6f830e339a7a9029cc25bc26 Mon Sep 17 00:00:00 2001
From: Dave Rigby <daver@couchbase.com>
Date: Tue, 16 May 2017 11:37:31 +0100
Subject: [PATCH] MB-22602: Replace memcached,ep_engine subdirs with kv_engine

Replace the separate memcached & ep_engine subdirectories with the
newly-unified kv_engine subdir.

Change-Id: Ic87bc4d99f18e06f13bd62d4da370f38180e75f9
---
 CMakeLists.txt | 5 ++---
 1 file changed, 2 insertions(+), 3 deletions(-)

diff --git a/CMakeLists.txt b/CMakeLists.txt
index 4f0c8e1..bdb2f98 100644
--- a/CMakeLists.txt
+++ b/CMakeLists.txt
@@ -196,8 +196,7 @@ ENDIF (PHOSPHOR_DISABLE)

 ADD_SUBDIRECTORY(platform)
 ADD_SUBDIRECTORY(subjson)
-ADD_SUBDIRECTORY(memcached/include)
-ADD_SUBDIRECTORY(memcached)
+ADD_SUBDIRECTORY(kv_engine/include)
 ADD_SUBDIRECTORY(couchstore)
 IF (NOT COUCHBASE_KV_COMMIT_VALIDATION)
     ADD_SUBDIRECTORY(forestdb)
@@ -206,7 +205,7 @@ IF (NOT COUCHBASE_KV_COMMIT_VALIDATION)
     GET_FILENAME_COMPONENT (FORESTDB_LIBRARY_DIR "${_dir}" PATH)
     SET (FORESTDB_TARGET forestdb)
 ENDIF()
-ADD_SUBDIRECTORY(ep-engine)
+ADD_SUBDIRECTORY(kv_engine)
 ADD_SUBDIRECTORY(sigar)
 ADD_SUBDIRECTORY(moxi)

--
2.10.0

EOF
popd
cp -f ../tlm/CMakeLists.txt ..
