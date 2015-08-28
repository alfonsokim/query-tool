
__all__ = ['_debug', 'STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME', 
           'COLUMNS', 'INDEX_COLUMNS', 'column_by_name', '_error']

from collections import namedtuple
import sys

## ============================================================================
Column = namedtuple('Column', 'name index size')
STB = Column('STB', 0, 64)
TITLE = Column('TITLE', 1, 64)
PROVIDER = Column('PROVIDER', 2, 64)
DATE = Column('DATE', 3, 10)
REV = Column('REV', 4, 10)
VIEW_TIME = Column('VIEW_TIME', 5, 10)
COLUMNS = [STB, TITLE, PROVIDER, DATE, REV, VIEW_TIME]
INDEX_COLUMNS = [STB.index, TITLE.index]

## ============================================================================
def _debug(message, options):
    """
    """
    if options.verbose:
        print >> sys.stderr, message

## ============================================================================
def _error(message, options):
    """
    """
    print >> sys.stderr, message
    sys.exit(1)

## ============================================================================
def column_by_name(column_name):
    for column in COLUMNS:
        if column_name.upper() == column.name:
            return column
    return False