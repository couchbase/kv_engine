#!/usr/bin/env python

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

######################################################################
# Effects
######################################################################

class StoreEffect(Effect):

    def __init__(self, v='0'):
        self.v = v

    def __call__(self, state):
        state[TESTKEY] = self.v

class LockEffect(Effect):

    def __call__(self, state):
        state[TESTKEY + '.lock'] = True

class UnlockEffect(Effect):

    def __call__(self, state):
        klock = TESTKEY + '.lock'
        if klock in state:
            del state[klock]

class DeleteEffect(Effect):

    def __call__(self, state):
        del state[TESTKEY]
        klock = TESTKEY + '.lock'
        if klock in state:
            del state[klock]

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

    preconditions = [IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class Add(Action):

    preconditions = [DoesNotExistCondition(), IsUnlockedCondition()]
    effect = StoreEffect()
    postconditions = [ExistsCondition()]

class Delete(Action):

    preconditions = [ExistsCondition(), IsUnlockedCondition()]
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

class EngineTestAppDriver(Driver):

    def __init__(self, writer=sys.stdout):
        self.writer = writer

    def output(self, s):
        self.writer.write(s)

    def preSuite(self, seq):
        self.output('#include "suite_stubs.h"\n\n')

    def testName(self, seq):
        return 'test_' + '_'.join(a.name for a in seq)

    def shouldRunSequence(self, seq):
        # Skip any sequence that leads to known failures
        ok = True
        sclasses = [type(a) for a in seq]
        # A list of lists of classes such that any test that includes
        # the inner list in order should be skipped.
        bads = [[GetLock, Append], [GetLock, Prepend]]
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

    def postSuite(self, seq):
        self.output("""MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {

    static engine_test_t tests[]  = {

""")
        for seq in sorted(seq):
            self.output('        {"%s",\n         %s,\n         NULL, teardown, NULL},\n' % (
                    ', '.join(a.name for a in seq),
                    self.testName(seq)))

        self.output("""        {NULL, NULL, NULL, NULL, NULL}
    };
    return tests;
}
""")

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
            vs = ' // value is "%s"\n' % value
        else:
            vs = ' // value is not defined\n'

        if errored:
            self.output("    assertHasError();" + vs)
        else:
            self.output("    assertHasNoError();" + vs)

if __name__ == '__main__':
    breakdancer.runTest(breakdancer.findActions(globals().values()),
                        EngineTestAppDriver(sys.stdout))
