#
# what to dump
#
class Phase(object):
    # prepare
    SCHEMA = 1
    # execute
    DATA = 2
    # finalize
    INDICES = 4
    STATISTICS = 8
    #
    ALL = SCHEMA + DATA + INDICES + STATISTICS
    # nicknames
    PREPARE = SCHEMA
    EXECUTE = DATA
    FINALIZE = INDICES + STATISTICS
