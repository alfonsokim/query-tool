
__all__ = ['_debug', 'STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME', 
           'COLUMNS', 'column_by_name', '_error', 'ROW_SIZE',
           'column_by_position', 'SelectColumn']

from datetime import datetime as dt
from collections import namedtuple
import sys

## ============================================================================
def char(value):
    return value

def date(value):
    #print value.__class__
    return dt.strptime(value, '%Y-%m-%d')

def money(value):
    dollars, cents = tuple(value.strip('.'))
    return (dollars * 100) + cents

def time(value):
    hours, minutes = tuple(value.strip(':'))
    return (hours * 60) + minutes


## ============================================================================
Column = namedtuple('Column', 'name index is_index size offset type')
## ----------------------------------------------------------------------------
## ---- Column definition ----
STB = Column(name='STB', index=0, is_index=True, size=64, offset=0, type=char)
TITLE = Column(name='TITLE', index=1, is_index=True, size=64, offset=64, type=char)
PROVIDER = Column(name='PROVIDER', index=2, is_index=False, size=64, offset=128, type=char)
DATE = Column(name='DATE', index=3, is_index=True, size=10, offset=192, type=date)
REV = Column(name='REV', index=4, is_index=False, size=10, offset=202, type=money)
VIEW_TIME = Column(name='VIEW_TIME', index=5, is_index=False, size=10, offset=212, type=time)
COLUMNS = [STB, TITLE, PROVIDER, DATE, REV, VIEW_TIME]
ROW_SIZE = sum([c.size for c in COLUMNS])

AGGREGATES = 'min,max,sum,count,collect,'.split(',')
COLUMN_PROPERTIES = 'name index is_index size offset'.split()

## ============================================================================
class SelectColumn(): ## TODO: Cambiar de lugar esta clase
    """
    """

    # -------------------------------------------------------------------------
    def __init__(self, column, aggregate=''):
        """
        """
        self.column = column
        self.aggregate = aggregate
        self.values = []
        if aggregate not in AGGREGATES:
            _error('invalid aggregate: %s' % aggregate, {})

    # -------------------------------------------------------------------------
    def __getattr__(self, attr):
        """ Proxy for the column properties
        """
        return self.column[COLUMN_PROPERTIES.index(attr)]

    # -------------------------------------------------------------------------
    def add_value(self, value):
        """
        """
        values = self.values
        if self.aggregate == '':
            values.append(value)
        if self.aggregate == 'collect':
            if len(values) == 0:
                values.append(set([value]))
            else:
                values[0].add(value)
        if self.aggregate == 'count':
            if len(values) == 0:
                values.append(1)
            else:
                values[0] += 1
        new_val = self.column.type(value)
        last_val = self.column.type(values[0]) if len(values) > 0 else new_val
        if self.aggregate == 'sum':
            if len(values) == 0:
                values.append(new_val)
            else:
                values[0] += new_val
        if self.aggregate == 'min' and new_val < last_val:
            if len(values) == 0:
                values.append(value)
            else:
                values[0] = value
        if self.aggregate == 'max' and new_val > last_val:
            if len(values) == 0:
                values.append(value)
            else:
                values[0] = value
        if len(values) == 0:
            values.append(value)
        self.values = values

    # -------------------------------------------------------------------------
    def format_name(self):
        """
        """
        if self.aggregate != '':
            return ':'.join([self.column.name, self.aggregate])
        else:
            return self.column.name


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