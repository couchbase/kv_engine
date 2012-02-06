#!/usr/bin/env python

import os
import sys
import exceptions
import string
import traceback

try:
    import sqlite3
except:
    sys.exit("ERROR: backup_util requires python version 2.6 or greater")


def compare_backup_files(bfile1, bfile2):
    if (len(bfile1) != 2 or len(bfile2) != 2):
        exit("ERROR: each backup file record should consist of a file name and the map where " \
             "key is vbucket_id and value is the list of checkpoint ids")
    bfile_name1 = bfile1[0]
    bfile_name2 = bfile2[0]
    first_comp_result = 0
    common_vb_counter = 0
    vb_chks1 = bfile1[1]
    vb_chks2 = bfile2[1]
    for (vbid, chks1) in vb_chks1.iteritems():
        if len(chks1) == 0:
            continue
        if vbid in vb_chks2:
            chks2 = vb_chks2[vbid]
            if len(chks2) == 0:
                continue
            common_vb_counter = common_vb_counter + 1
            # Compare the end_checkpoint_id of a vbucket in the first backup file with
            # the begin_checkpoint_id of the same vbucket in the second backup file.
            comp_result = chks1[len(chks1) - 1] - chks2[0]
            if common_vb_counter == 1:
                first_comp_result = comp_result
            else:
                if (comp_result > 0 and first_comp_result <= 0) or \
                   (comp_result == 0 and first_comp_result != 0) or \
                   (comp_result < 0 and first_comp_result >= 0):
                    print "Backup files '%s' and '%s' are not comparable by vb checkpoint ids!!!" \
                          % (bfile_name1, bfile_name2)
                    exit("ERROR: backup files are not comparable by their vb checkpoint ids!!!")
    return first_comp_result

def validate_incremental_backup_files(backup_files):
    db = None
    # Key is a backup file name and value is another map where key is vbucket_id and
    # value is the list of checkpoint ids.
    bfile_chks = {}
    # Key is vbucket_id and value is the list of checkpoint ids.
    vbid_chks = {}
    try:
        # First, extract the list of all closed checkpoint ids from each backup file.
        for bfile in backup_files:
            if os.path.exists(bfile) == False:
                exit("ERROR: backup file '%s' does not exist!!!" % (bfile))
            db = sqlite3.connect(bfile)
            db.text_factory = str
            c = db.cursor()
            c.execute("select vbucket_id, cpoint_id from cpoint_state " +
                      "where state=\"closed\" order by vbucket_id asc, cpoint_id asc")
            vb_checkpoints = {} # key is vbucket_id and value is the list of checkpoint ids.
            curr_vb_id = -1
            curr_chk_id = -1
            for row in c:
                vbid = row[0]
                chkid = row[1]
                if not vbid in vbid_chks:
                    vbid_chks[vbid] = [chkid]
                else:
                    vbid_chks[vbid].append(chkid)
                if not vbid in vb_checkpoints:
                    vb_checkpoints[vbid] = [chkid]
                else:
                    vb_checkpoints[vbid].append(chkid)
            if vb_checkpoints != {} :
                bfile_chks[bfile] = vb_checkpoints
            db.close()

        # Check if there are any missing checkpoints in each vbucket
        checkpoint_missing = False
        for (vbid, chks) in vbid_chks.iteritems():
            chk_list_len = len(chks)
            if chk_list_len == 0:
                continue
            chks.sort()
            for i in range(chk_list_len - 1):
                if chks[i] == chks[i+1] or chks[i] + 1 == chks[i+1]:
                    continue
                else:
                    checkpoint_missing = True
                    print "Missing checkpoint ids from %d to %d for vbucket %d" \
                          % (chks[i] + 1, chks[i+1] - 1, vbid)
        if checkpoint_missing == True:
            exit("Error: Some checkpoint ids are missing from incremental backup files!!!")

        backup_chks_items = []
        for bf in backup_files:
            backup_chks_items.append((bf, bfile_chks[bf]))

        # Sort backup files by the descending order of their checkpoints ids for vbuckets
        sorted_bfile_chks = sorted(backup_chks_items, cmp=compare_backup_files, reverse=True)
        sorted_backup_files = []
        for bfile in sorted_bfile_chks:
            sorted_backup_files.append(bfile[0])
        return sorted_backup_files

    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        exit("ERROR: " + str(e))
