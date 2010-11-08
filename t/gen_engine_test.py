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

class EngineTestAppDriver(Driver):

    def __init__(self, writer=sys.stdout):
        self.writer = writer

    def output(self, s):
        self.writer.write(s)

    def preSuite(self, seq):
        self.output('#include "suite_stubs.h"\n\n')

    def testName(self, seq):
        return 'test_' + '_'.join(a.name for a in seq)

    def startSequence(self, seq):
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
