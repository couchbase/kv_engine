#!/usr/bin/env python

import os
import sys

import breakdancer
from breakdancer import Condition, Effect, Action, Driver

TESTKEY = 'testkey'

######################################################################
# Conditions
######################################################################

class ExistsCondition(Condition):

    def __call__(self, state):
        return TESTKEY in state

class ExistsAsNumber(Condition):

    def __call__(self, state):
        try:
            int(state[TESTKEY])
            return True
        except:
            return False

class MaybeExistsAsNumber(ExistsAsNumber):

    def __call__(self, state):
        return TESTKEY not in state or ExistsAsNumber.__call__(self, state)

class DoesNotExistCondition(Condition):

    def __call__(self, state):
        return TESTKEY not in state

class NothingExistsCondition(Condition):

    def __call__(self, state):
        return not bool(state)

class IsLockedCondition(Condition):

    def __call__(self, state):
        return (TESTKEY + '.lock') in state

class IsUnlockedCondition(Condition):

    def __call__(self, state):
        return (TESTKEY + '.lock') not in state

class KnowsCAS(Condition):

    def __call__(self, state):
        return (TESTKEY + '.cas') in state

######################################################################
# Effects
######################################################################

class NoFX(Effect):

    def __call__(self, state):
        pass

class MutationEffect(Effect):

    def forgetCAS(self, state):
        k = TESTKEY + '.cas'
        if k in state:
            del state[k]

class StoreEffect(MutationEffect):

    def __init__(self, rememberCAS=False):
        self.rememberCAS = rememberCAS

    def __call__(self, state):
        state[TESTKEY] = '0'
        if self.rememberCAS:
            state[TESTKEY + '.cas'] = 1
        else:
            self.forgetCAS(state)

class LockEffect(MutationEffect):

    def __call__(self, state):
        state[TESTKEY + '.lock'] = True
        self.forgetCAS(state)

class UnlockEffect(Effect):

    def __call__(self, state):
        klock = TESTKEY + '.lock'
        if klock in state:
            del state[klock]

class DeleteEffect(MutationEffect):

    def __call__(self, state):
        del state[TESTKEY]
        klock = TESTKEY + '.lock'
        if klock in state:
            del state[klock]
        self.forgetCAS(state)

class FlushEffect(Effect):

    def __call__(self, state):
        state.clear()

class AppendEffect(MutationEffect):

    suffix = '-suffix'

    def __call__(self, state):
        state[TESTKEY] = state[TESTKEY] + self.suffix
        self.forgetCAS(state)

class PrependEffect(MutationEffect):

    prefix = 'prefix-'

    def __call__(self, state):
        state[TESTKEY] = self.prefix + state[TESTKEY]
        self.forgetCAS(state)

class ArithmeticEffect(MutationEffect):

    default = '0'

    def __init__(self, by=1):
        self.by = by

    def __call__(self, state):
        if TESTKEY in state:
            state[TESTKEY] = str(max(0, int(state[TESTKEY]) + self.by))
        else:
            state[TESTKEY] = self.default
        self.forgetCAS(state)

######################################################################
# Actions
######################################################################

class Get(Action):

    preconditions = [ExistsCondition()]
    effect = NoFX()

class Set(Action):

    preconditions = [IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class setUsingCAS(Action):

    preconditions = [KnowsCAS(), IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class SetRetainCAS(Action):

    preconditions = [IsUnlockedCondition()]
    effect = StoreEffect(True)
    postconditions = [ExistsCondition(), KnowsCAS()]

class Add(Action):

    preconditions = [DoesNotExistCondition(), IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class Delete(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
    effect = DeleteEffect()
    postconditions = [DoesNotExistCondition()]

class DeleteUsingCAS(Action):

    preconditions = [KnowsCAS(), ExistsCondition(), IsUnlockedCondition()]
    effect = DeleteEffect()
    postconditions = [DoesNotExistCondition()]

class Flush(Action):

    effect = FlushEffect()
    postconditions = [NothingExistsCondition()]

class WaitForLock(Action):

    effect = UnlockEffect()

class GetLock(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
    effect = LockEffect()
    postconditions = [IsLockedCondition()]

class Delay(Flush):
    pass

class Append(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
    effect = AppendEffect()
    postconditions = [ExistsCondition()]

class Prepend(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
    effect = PrependEffect()
    postconditions = [ExistsCondition()]

class AppendUsingCAS(Action):

    preconditions = [KnowsCAS(), ExistsCondition(), IsUnlockedCondition()]
    effect = AppendEffect()
    postconditions = [ExistsCondition()]

class PrependUsingCAS(Action):

    preconditions = [KnowsCAS(), ExistsCondition(), IsUnlockedCondition()]
    effect = PrependEffect()
    postconditions = [ExistsCondition()]

class Incr(Action):

    preconditions = [ExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(1)
    postconditions = [ExistsAsNumber()]

class Decr(Action):

    preconditions = [ExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(-1)
    postconditions = [ExistsAsNumber()]

class IncrWithDefault(Action):

    preconditions = [MaybeExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(1)
    postconditions = [ExistsAsNumber()]

class DecrWithDefault(Action):

    preconditions = [MaybeExistsAsNumber(), IsUnlockedCondition()]
    effect = ArithmeticEffect(-1)
    postconditions = [ExistsAsNumber()]

######################################################################
# Driver
######################################################################

class TestFile(object):

    def __init__(self, path, n=10):
        self.tmpfilenames = ["%s_%d.c.tmp" % (path, i) for i in range(n)]
        self.files = [open(tfn, "w") for tfn in self.tmpfilenames]
        self.seq = [list() for f in self.files]
        self.index = 0

    def finish(self):
        for f in self.files:
            f.close()

        for tfn in self.tmpfilenames:
            nfn = tfn[:-4]
            assert (nfn + '.tmp') == tfn
            os.rename(tfn, nfn)

    def nextfile(self):
        self.index = self.index + 1
        if self.index >= len(self.files):
            self.index = 0

    def write(self, s):
        self.files[self.index].write(s)

    def addseq(self, seq):
        self.seq[self.index].append(seq)

class EngineTestAppDriver(Driver):

    def __init__(self, writer=sys.stdout):
        self.writer = writer

    def output(self, s):
        self.writer.write(s)

    def preSuite(self, seq):
        files = [self.writer]
        if isinstance(self.writer, TestFile):
            files = self.writer.files
        for f in files:
            f.write('#include "tests/suite_stubs.h"\n\n')

    def testName(self, seq):
        return 'test_' + '_'.join(a.name for a in seq)

    def shouldRunSequence(self, seq):
        # Skip any sequence that leads to known failures
        ok = True
        sclasses = [type(a) for a in seq]
        # A list of lists of classes such that any test that includes
        # the inner list in order should be skipped.
        bads = []
        for b in bads:
            try:
                nextIdx = 0
                for c in b:
                    nextIdx = sclasses.index(c, nextIdx) + 1
                ok = False
            except ValueError:
                pass # Didn't find it, move in

        return ok

    def startSequence(self, seq):
        if isinstance(self.writer, TestFile):
            self.writer.nextfile()
            self.writer.addseq(seq)

        f = "static enum test_result %s" % self.testName(seq)
        self.output(("%s(ENGINE_HANDLE *h,\n%sENGINE_HANDLE_V1 *h1) {\n"
                     % (f, " " * (len(f) + 1))))

        self.handled = self.shouldRunSequence(seq)

        if not self.handled:
            self.output("    (void)h;\n")
            self.output("    (void)h1;\n")
            self.output("    return PENDING;\n")
            self.output("}\n\n")

    def startAction(self, action):
        if not self.handled:
            return

        if isinstance(action, Delay):
            s = "    delay(expiry+1);"
        elif isinstance(action, WaitForLock):
            s = "    delay(locktime+1);"
        elif isinstance(action, Flush):
            s = "    flush(h, h1);"
        elif isinstance(action, Delete):
            s = '    del(h, h1);'
        else:
            s = '    %s(h, h1);' % (action.name)
        self.output(s + "\n")

    def _writeList(self, writer, fname, seq):
        writer.write("""MEMCACHED_PUBLIC_API
engine_test_t* %s(void) {

    static engine_test_t tests[]  = {

""" % fname)
        for seq in sorted(seq):
            writer.write('        {"%s",\n         %s,\n         NULL, teardown, NULL, NULL, NULL},\n' % (
                    ', '.join(a.name for a in seq),
                    self.testName(seq)))

        writer.write("""        {NULL, NULL, NULL, NULL, NULL, NULL, NULL}
    };
    return tests;
}
""")

    def postSuite(self, seq):
        if isinstance(self.writer, TestFile):
            for i in range(len(self.writer.files)):
                self._writeList(self.writer.files[i],
                                'get_tests_%d' % i,
                                self.writer.seq[i])
        else:
            self._writeList(self.writer, 'get_tests', seq)

    def endSequence(self, seq, state):
        if not self.handled:
            return

        val = state.get(TESTKEY)
        if val:
            self.output('    checkValue(h, h1, "%s");\n' % val)
        else:
            self.output('    assertNotExists(h, h1);\n')
        self.output("    return SUCCESS;\n")
        self.output("}\n\n")

    def endAction(self, action, state, errored):
        if not self.handled:
            return

        value = state.get(TESTKEY)
        if value:
            vs = ' /* value is "%s" */\n' % value
        else:
            vs = ' /* value is not defined */\n'

        if errored:
            self.output("    assertHasError();" + vs)
        else:
            self.output("    assertHasNoError();" + vs)

if __name__ == '__main__':
    w = TestFile('generated_suite')
    breakdancer.runTest(breakdancer.findActions(globals().values()),
                        EngineTestAppDriver(w))
    w.finish()
