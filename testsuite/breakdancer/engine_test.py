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

######################################################################
# Effects
######################################################################

class StoreEffect(Effect):

    def __init__(self, v='0'):
        self.v = v

    def __call__(self, state):
        state[TESTKEY] = self.v

class DeleteEffect(Effect):

    def __call__(self, state):
        del state[TESTKEY]

class FlushEffect(Effect):

    def __call__(self, state):
        state.clear()

class AppendEffect(Effect):

    suffix = '-suffix'

    def __call__(self, state):
        state[TESTKEY] = state[TESTKEY] + self.suffix

class PrependEffect(Effect):

    prefix = 'prefix-'

    def __call__(self, state):
        state[TESTKEY] = self.prefix + state[TESTKEY]

class ArithmeticEffect(Effect):

    default = '0'

    def __init__(self, by=1):
        self.by = by

    def __call__(self, state):
        if TESTKEY in state:
            state[TESTKEY] = str(max(0, int(state[TESTKEY]) + self.by))
        else:
            state[TESTKEY] = self.default

######################################################################
# Actions
######################################################################

class Set(Action):

    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class Add(Action):

    preconditions = [DoesNotExistCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class Delete(Action):

    preconditions = [ExistsCondition()]
    effect = DeleteEffect()
    postconditions = [DoesNotExistCondition()]

class Flush(Action):

    effect = FlushEffect()
    postconditions = [NothingExistsCondition()]

class Delay(Flush):
    pass

class Append(Action):

    preconditions = [ExistsCondition()]
    effect = AppendEffect()
    postconditions = [ExistsCondition()]

class Prepend(Action):

    preconditions = [ExistsCondition()]
    effect = PrependEffect()
    postconditions = [ExistsCondition()]

class Incr(Action):

    preconditions = [ExistsAsNumber()]
    effect = ArithmeticEffect(1)
    postconditions = [ExistsAsNumber()]

class Decr(Action):

    preconditions = [ExistsAsNumber()]
    effect = ArithmeticEffect(-1)
    postconditions = [ExistsAsNumber()]

class IncrWithDefault(Action):

    preconditions = [MaybeExistsAsNumber()]
    effect = ArithmeticEffect(1)
    postconditions = [ExistsAsNumber()]

class DecrWithDefault(Action):

    preconditions = [MaybeExistsAsNumber()]
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
            if os.path.exists(nfn):
                os.remove(nfn)
            os.rename(tfn, nfn)

    def nextfile(self):
        self.index += 1
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
            f.write('/* DO NOT EDIT.. GENERATED SOURCE */\n\n')
            f.write('#include "testsuite/breakdancer/disable_optimize.h"\n')
            f.write('#include "testsuite/breakdancer/suite_stubs.h"\n\n')

    def testName(self, seq):
        return 'test_' + '_'.join(a.name for a in seq)

    def startSequence(self, seq):
        if isinstance(self.writer, TestFile):
            self.writer.nextfile()
            self.writer.addseq(seq)

        f = "static enum test_result %s" % self.testName(seq)
        self.output(("%s(ENGINE_HANDLE *h,\n%sENGINE_HANDLE_V1 *h1) {\n"
                     % (f, " " * (len(f) + 1))))


    def startAction(self, action):
        if isinstance(action, Delay):
            s = "    delay(expiry+1);"
        elif isinstance(action, Flush):
            s = "    flush(h, h1);"
        elif isinstance(action, Delete):
            s = '    del(h, h1);'
        else:
            s = '    %s(h, h1);' % (action.name)
        self.output(s + "\n")

    def _writeList(self, writer, fname, seq):
        writer.write("""engine_test_t* %s(void) {
    static engine_test_t tests[]  = {

""" % fname)
        for seq in sorted(seq):
            writer.write('        TEST_CASE("%s",\n         %s,\n         test_setup, teardown, NULL, NULL, NULL),\n' % (
                    ', '.join(a.name for a in seq),
                    self.testName(seq)))

        writer.write("""        TEST_CASE(NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    };
    return tests;
}
""")

    def postSuite(self, seq):
        if isinstance(self.writer, TestFile):
            for i, v in enumerate(self.writer.files):
                self._writeList(v, 'get_tests', self.writer.seq[i])
        else:
            self._writeList(self.writer, 'get_tests', seq)

    def endSequence(self, seq, state):
        val = state.get(TESTKEY)
        if val:
            self.output('    checkValue(h, h1, "%s");\n' % val)
        else:
            self.output('    assertNotExists(h, h1);\n')
        self.output("    return SUCCESS;\n")
        self.output("}\n\n")

    def endAction(self, action, state, errored):
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
