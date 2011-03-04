#!/usr/bin/env python

import sys

sys.path.append('management')

import os
import time
import random
import shutil
import socket
import struct
import tempfile
import unittest

import mc_bin_client
import memcacheConstants
import util

try:
    import sqlite3
except:
    sys.exit("%s requires python version 2.6 or greater" %
              (os.path.basename(sys.argv[0])))

MBI = "./management/mbbackup-incremental"

KEY_PREFIX = "hello-\x01\x00\xff-"
VAL_PREFIX = "world\x00\xff-"

def listen(port=11310):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', port))
    s.listen(1)
    return s

class MockTap(mc_bin_client.MemcachedClient):
    """Wraps a server-side, accepted socket,
       so we can mock up tap server interactions."""

    def __init__(self, testCase, accepted):
        self.t = testCase
        self.a = accepted
        self.s = accepted[0]
        self.r = random.Random()
        self.t.assertNotEqual(self.s, None)

    def readMsg(self):
        ext = ''
        key = ''
        val = ''
        cmd, status, opaque, cas, keylen, extlen, data = self._recvMsg()
        if data:
            ext = data[0:extlen]
            key = data[extlen:extlen+keylen]
            val = data[extlen+keylen:]
        return cmd, opaque, cas, status, key, ext, val

    def sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0, vbucketId=0):
        self.vbucketId = vbucketId
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)

    def sendStartCheckpoint(self, vbucketId, checkpointId):
        self.sendMsg(memcacheConstants.CMD_TAP_CHECKPOINT_START,
                     '', str(checkpointId), 0, vbucketId=vbucketId)

    def sendEndCheckpoint(self, vbucketId, checkpointId):
        self.sendMsg(memcacheConstants.CMD_TAP_CHECKPOINT_END,
                     '', str(checkpointId), 0, vbucketId=vbucketId,
                     extraHeader=struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                                             0, memcacheConstants.TAP_FLAG_ACK, 0))

    def sendTapMutation(self, vbucketId, key, val, flg=0x12345678, exp=0x11223344,
                        cas=0x1020304050607080L):
        self.sendMsg(memcacheConstants.CMD_TAP_MUTATION,
                     key, val, 0, vbucketId=vbucketId, cas=cas,
                     extraHeader=struct.pack(memcacheConstants.TAP_MUTATION_PKT_FMT,
                                             0, 0, 0, flg, exp))
    def sendTapGeneral(self, cmd, vbucketId, key, val='',
                       cas=0x1120334055607780L):
        self.sendMsg(cmd,
                     key, val, 0, vbucketId=vbucketId, cas=cas,
                     extraHeader=struct.pack(memcacheConstants.TAP_GENERAL_PKT_FMT,
                                             0, 0, 0))

class TestIncrementalBackup(unittest.TestCase):

    def setUp(self):
        self.sent = {
            'mutations': {}, # Key is vbucketId, value is int counter.
            'deletes': {}, # Key is vbucketId, value is int counter.
            'flush_alls': {}, # Key is vbucketId, value is int counter.
            'checkpoints': {}, # Key is vbucketId, value is int counter.
        }

    def addSent(self, kind, vbucketId):
        v = self.sent[kind] = self.sent.get(kind, {})
        v[vbucketId] = v.get(vbucketId, 0) + 1

    def test_compute_next_file(self):
        d = tempfile.mkdtemp()
        self.assertEqual(d + "/file-00000.mbb",
                         util.expand_file_pattern(d + "/file-%.mbb"))
        open(d + "/file-00000.mbb", 'w').close()
        self.assertEqual(d + "/file-00001.mbb",
                         util.expand_file_pattern(d + "/file-%.mbb"))
        open(d + "/file-00001.mbb", 'w').close()
        self.assertEqual(d + "/file-00002.mbb",
                         util.expand_file_pattern(d + "/file-%.mbb"))
        open(d + "/file-00009.mbb", 'w').close()
        self.assertEqual(d + "/file-00010.mbb",
                         util.expand_file_pattern(d + "/file-%.mbb"))
        self.assertEqual(d + "/diff-00000.mbb",
                         util.expand_file_pattern(d + "/diff-%.mbb"))
        shutil.rmtree(d)

    def test_server_unconnectable(self):
        self.assertNotEqual(0,
                            os.system(MBI
                                      + " -h fake-server:6666 xxx"
                                      + " 2> /dev/null"))

    def prep(self):
        d = tempfile.mkdtemp()
        o = d + "/out"
        s = listen()
        os.system(MBI + " -v -v -v -T 0.1"
                      + " -h 127.0.0.1:11310"
                      + " -o " + d + "/backup.mbb"
                      + " client0"
                      + " >> " + o + " 2>&1 &")
        mt = self.expectClient(s)
        return mt, d, o, s

    def cleanup(self, mt, d, o, s, expectError=False, errorString=None):
        mt.close()
        time.sleep(0.2)
        lines = open(o).readlines()
        self.assertEqual(expectError,
                         "\n".join(lines).find("ERROR:") != -1)
        if expectError and errorString:
            self.assertTrue("\n".join(lines).find(errorString) > 0)
        self.assertTrue(len(lines) > 0)
        self.assertTrue(os.path.exists(d + "/backup.mbb"))
        if not expectError:
            db = sqlite3.connect(d + "/backup.mbb")
            for vbucketId, counter in self.sent['checkpoints'].items():
                for r in db.execute("select count(*) from cpoint_state where vbucket_id = ?",
                                    (vbucketId,)):
                    self.assertEqual(r[0], counter)
            for vbucketId, counter in self.sent['mutations'].items():
                for r in db.execute("select count(*) from cpoint_op where op = \"m\" and vbucket_id = ?",
                                    (vbucketId,)):
                    self.assertEqual(r[0], counter)
            for vbucketId, counter in self.sent['deletes'].items():
                for r in db.execute("select count(*) from cpoint_op where op = \"d\" and vbucket_id = ?",
                                    (vbucketId,)):
                    self.assertEqual(r[0], counter)
            for vbucketId, counter in self.sent['flush_alls'].items():
                for r in db.execute("select count(*) from cpoint_op where op = \"f\" and vbucket_id = ?",
                                    (vbucketId,)):
                    self.assertEqual(r[0], counter)
            for r in db.execute("select count(*) from cpoint_op where op = \"m\" and flg != ?",
                                (0x12345678,)):
                self.assertEqual(r[0], 0)
            for r in db.execute("select count(*) from cpoint_op where op = \"m\" and exp != ?",
                                (0x11223344,)):
                self.assertEqual(r[0], 0)
            for r in db.execute("select count(*) from cpoint_op where op = \"m\" and key not like ?",
                                (KEY_PREFIX + "%",)):
                self.assertEqual(r[0], 0)
            for r in db.execute("select count(*) from cpoint_op where op = \"m\" and val != ?",
                                (VAL_PREFIX + "bye",)):
                self.assertEqual(r[0], 0)
            for r in db.execute("select count(*) from cpoint_op where op = \"m\" and cas != ?",
                                (0x1020304050607080L,)):
                self.assertEqual(r[0], 0)
            for r in db.execute("select count(*) from cpoint_op where op = \"d\" and cas != ?",
                                (0x1120334055607780L,)):
                self.assertEqual(r[0], 0)
            db.close()
        shutil.rmtree(d)

    def expectClient(self, s, clientName="client0"):
        mt = MockTap(self, s.accept())
        cmd, opaque, cas, status, key, ext, val = mt.readMsg()
        self.assertEqual(cmd, memcacheConstants.CMD_TAP_CONNECT)
        self.assertEqual(key, clientName)
        flags = struct.unpack(">I", ext)[0]
        self.assertEqual(flags,
                         memcacheConstants.TAP_FLAG_CHECKPOINT |
                         memcacheConstants.TAP_FLAG_SUPPORT_ACK |
                         memcacheConstants.TAP_FLAG_REGISTERED_CLIENT)
        return mt

    def expectAckCheckpoint(self, mt, vbucketId, checkpointId):
        cmd, opaque, cas, vid, key, ext, val = mt.readMsg()
        self.assertEqual(cmd, memcacheConstants.CMD_TAP_CHECKPOINT_END)
        self.assertEqual(val, str(checkpointId))
        self.assertEqual(vid, vbucketId)
        self.addSent('checkpoints', vbucketId)

    def test_basic_tap_connect(self):
        try:
            mt, d, o, s = self.prep()
            self.cleanup(mt, d, o, s)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_good_checkpoint(self):
        self.test_good_checkpoints(n=1)

    def test_good_checkpoints(self, n=5, nMutations=1, nDeletes=0, nFlushAlls=0):
        try:
            mt, d, o, s = self.prep()
            for i in range(n):
                self.fullCheckpoint(mt, 0, "c" + str(i),
                                    nMutations=nMutations, nDeletes=nDeletes, nFlushAlls=nFlushAlls)
            self.cleanup(mt, d, o, s)
        finally:
            s.close()
        return
        try:
            pass
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def fullCheckpoint(self, mt, vbucketId, checkpointId, nMutations=1, nDeletes=0, nFlushAlls=0):
        mt.sendStartCheckpoint(vbucketId, checkpointId)
        for i in range(nMutations):
            mt.sendTapMutation(vbucketId, KEY_PREFIX + str(i), VAL_PREFIX + "bye")
            self.addSent('mutations', vbucketId)
        for i in range(nDeletes):
            mt.sendTapGeneral(memcacheConstants.CMD_TAP_DELETE,
                              vbucketId, "delete-\x01\x00-" + str(i))
            self.addSent('deletes', vbucketId)
        for i in range(nFlushAlls):
            mt.sendTapGeneral(memcacheConstants.CMD_TAP_FLUSH,
                              vbucketId, "flush-\x01\x00-" + str(i))
            self.addSent('flush_alls', vbucketId)
        mt.sendEndCheckpoint(vbucketId, checkpointId)
        self.expectAckCheckpoint(mt, vbucketId, checkpointId)

    def test_checkpoints_with_mutations(self):
        self.test_good_checkpoints(n=5, nMutations=5)

    def test_unexpected_mutation(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendTapMutation(0, "hello", "world")
            self.cleanup(mt, d, o, s, True)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_checkpoint_start_svrcrash(self, n=0, nMutations=1):
        try:
            mt, d, o, s = self.prep()
            for i in range(n):
                self.fullCheckpoint(mt, 0, "c" + str(i), nMutations)
            mt.sendStartCheckpoint(0, "c" + str(n))
            self.cleanup(mt, d, o, s)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_checkpoint_start_end_svrcrash(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendStartCheckpoint(0, "c0")
            mt.sendEndCheckpoint(0, "c0")
            self.expectAckCheckpoint(mt, 0, "c0")
            self.cleanup(mt, d, o, s, False)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_repeated_checkpoint_start_received(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendStartCheckpoint(0, "c0")
            mt.sendStartCheckpoint(0, "c1")
            self.cleanup(mt, d, o, s, True)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_interleaved_vbucket_messages(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendStartCheckpoint(0, "c0")
            mt.sendStartCheckpoint(1, "c0")
            mt.sendEndCheckpoint(0, "c0")
            mt.sendEndCheckpoint(1, "c0")
            self.expectAckCheckpoint(mt, 0, "c0")
            self.expectAckCheckpoint(mt, 1, "c0")
            self.cleanup(mt, d, o, s, False)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_TAP_CONNECT_error_name_already_in_use(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendMsg(memcacheConstants.CMD_TAP_CONNECT,
                       'tap name is already is use by another client',
                       '', 0, vbucketId=memcacheConstants.ERR_EXISTS)
            self.cleanup(mt, d, o, s, True, errorString='TAP_CONNECT error')
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_TAP_OPAQUE(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendStartCheckpoint(0, "c0")
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=0)
            mt.sendStartCheckpoint(1, "c0")
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=1)
            mt.sendEndCheckpoint(0, "c0")
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=0)
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=0)
            mt.sendEndCheckpoint(1, "c0")
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=1)
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=1)
            self.expectAckCheckpoint(mt, 0, "c0")
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=0)
            mt.sendMsg(memcacheConstants.CMD_TAP_OPAQUE,
                         'foo', "bar", 0, vbucketId=1)
            self.expectAckCheckpoint(mt, 1, "c0")
            self.cleanup(mt, d, o, s, False)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_unknown_TAP_message_received(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendStartCheckpoint(0, "c0")
            mt.sendMsg(memcacheConstants.CMD_GET,
                       'foo', "bar", 0, vbucketId=0)
            self.cleanup(mt, d, o, s, True)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_mutation_binary_checkpointId(self):
        try:
            mt, d, o, s = self.prep()
            mt.sendStartCheckpoint(0, "\x00")
            mt.sendStartCheckpoint(1, "\x01")
            mt.sendEndCheckpoint(0, "\x00")
            mt.sendEndCheckpoint(1, "\x01")
            self.expectAckCheckpoint(mt, 0, "\x00")
            self.expectAckCheckpoint(mt, 1, "\x01")
            self.cleanup(mt, d, o, s, False)
        except Exception as e:
            print d
            print "\n".join(open(o).readlines())
            raise e
        finally:
            s.close()

    def test_delete(self):
        self.test_good_checkpoints(n=5, nMutations=10, nDeletes=5)

    def test_flush_all(self):
        self.test_good_checkpoints(n=5, nMutations=10, nDeletes=5, nFlushAlls=2)

    def test_timeout_on_inactivity(self):
        todo()
    def test_pre_1_7_compatibility(self):
        todo()

def todo():
    pass # Flip this to assert(false) later.

if __name__ == '__main__':
    unittest.main()
