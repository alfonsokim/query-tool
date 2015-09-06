
__all__ = ['_debug', 'STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME', 
           'COLUMNS', 'column_by_name', '_error', 'ROW_SIZE',
           'column_by_position']

from collections import namedtuple
import sys

## ============================================================================
Column = namedtuple('Column', 'name index is_index size offset')
## ----------------------------------------------------------------------------
## ---- Column definition ----
STB = Column(name='STB', index=0, is_index=True, size=64, offset=0)
TITLE = Column(name='TITLE', index=1, is_index=True, size=64, offset=64)
PROVIDER = Column(name='PROVIDER', index=2, is_index=False, size=64, offset=128)
DATE = Column(name='DATE', index=3, is_index=True, size=10, offset=192)
REV = Column(name='REV', index=4, is_index=False, size=10, offset=202)
VIEW_TIME = Column(name='VIEW_TIME', index=5, is_index=False, size=10, offset=212)
COLUMNS = [STB, TITLE, PROVIDER, DATE, REV, VIEW_TIME]
ROW_SIZE = sum([c.size for c in COLUMNS])

## ============================================================================
def _debug(message, options, check_verbose=True):
    """
    """
    if not check_verbose:
        print >> sys.stderr, message
    elif check_verbose and options.verbose:
        print >> sys.stderr, message

## ============================================================================
def _error(message, options):
    """
    """
    print >> sys.stderr, message
    sys.exit(1)

## ============================================================================
def column_by_name(column_name, fail=False):
    for column in COLUMNS:
        if column_name.upper() == column.name:
            return column
    if fail: _error('Unknown column [%s]' % column_name, None)
    return False

## ============================================================================
def column_by_position(index, fail=False):
    for column in COLUMNS:
        if index == column.index:
            return column
    if fail: _error('Unknown column [%s]' % column_name, None)
    return False