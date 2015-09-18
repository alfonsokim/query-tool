
__all__ = ['STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME', 
           'COLUMNS', 'column_by_name', '_error', 'ROW_SIZE',
           '_debug', 'column_by_position', 'SelectColumn']

from datetime import datetime as dt
from collections import namedtuple
import sys

## ============================================================================
class Char():

    def __init__(self, value):
        self.value = value

    def format(self):
        return self.value

## ============================================================================
class Date():

    def __init__(self, value):
        self.value = dt.strptime(value, '%Y-%m-%d')

    def format(self):
        return self.value.strftime('%Y-%m-%d')

## ============================================================================
class Money():

    def __init__(self, value):
        dollars, cents = tuple(value.split('.'))
        self.value = (int(dollars) * 100) + int(cents)

    def format(self):
        return '%.2f' % (self.value / 100)

## ============================================================================
class Time():

    def __init__(self, value):
        hours, minutes = tuple(value.split(':'))
        self.value = (int(hours) * 60) + int(minutes)

    def format(self):
        return '%i:%02i' % (self.value / 60, self.value % 60)

## ============================================================================
Column = namedtuple('Column', 'name index is_index size offset type')
## ----------------------------------------------------------------------------
## ---- Column definition ----
STB = Column(name='STB', index=0, is_index=True, size=64, offset=0, type=Char)
TITLE = Column(name='TITLE', index=1, is_index=True, size=64, offset=64, type=Char)
PROVIDER = Column(name='PROVIDER', index=2, is_index=True, size=64, offset=128, type=Char)
DATE = Column(name='DATE', index=3, is_index=True, size=10, offset=192, type=Date)
REV = Column(name='REV', index=4, is_index=False, size=10, offset=202, type=Money)
VIEW_TIME = Column(name='VIEW_TIME', index=5, is_index=False, size=10, offset=212, type=Time)
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
        self.raw_values = None
        self.current_value = None
        if aggregate not in AGGREGATES:
            _error('invalid aggregate: %s' % aggregate)
        if aggregate == 'sum' and column.type in [Date, Char]:
            _error('Cannot add on column %s' % column.name)
        if aggregate == '':
            self.current_value = []

    # -------------------------------------------------------------------------
    def __getattr__(self, attr):
        """ Proxy for the column properties
        """
        return self.column[COLUMN_PROPERTIES.index(attr)]

    # -------------------------------------------------------------------------
    def __repr__(self):
        return 'Column: %s, aggregate: %s' % (self.column, self.aggregate)

    # -------------------------------------------------------------------------
    def add_value(self, value):
        """
        """
        new_value = self.column.type(value)
        if self.aggregate == '':
            self.current_value.append(new_value)
        elif self.aggregate in ['min', 'max', 'sum'] and not self.current_value:
            self.current_value = new_value
        elif self.aggregate == 'min' and new_value.value < self.current_value.value:
            self.current_value = new_value
        elif self.aggregate == 'max' and new_value.value > self.current_value.value:
            self.current_value = new_value
        elif self.aggregate == 'sum':
            self.current_value.value += new_value.value
        elif self.aggregate == 'count':
            if not self.current_value:
                self.current_value = 1
            else:
                self.current_value += 1
        elif self.aggregate == 'collect':
            if not self.current_value: 
                self.raw_values = set([new_value.value])
                self.current_value = [new_value] 
            else: 
                if new_value.value not in self.raw_values:
                    self.raw_values.add(new_value.value)
                    self.current_value.append(new_value)

    # -------------------------------------------------------------------------
    def values(self):
        """
        """
        if self.aggregate == '':
            return [v.format() for v in self.current_value]
        if self.aggregate in ['min', 'max', 'sum']:
            return [self.current_value.format()]
        if self.aggregate == 'count':
            return ['%i' % self.current_value]
        if self.aggregate == 'collect':
            return ['[%s]' % ','.join([v.format() for v in self.current_value])]

    # -------------------------------------------------------------------------
    def format_name(self):
        """
        """
        if self.aggregate != '':
            return ':'.join([self.column.name, self.aggregate])
        else:
            return self.column.name

## ============================================================================
def _debug(message, is_verbose):
    """
    """
    if is_verbose:
        print >> sys.stderr, 'DEBUG: %s' % message

## ============================================================================
def _error(message):
    """
    """
    print >> sys.stderr, message
    sys.exit(1)

## ============================================================================
def column_by_name(column_name, fail=False):
    for column in COLUMNS:
        if column_name.upper() == column.name:
            return column
    if fail: _error('Unknown column [%s]' % column_name)
    return False

## ============================================================================
def column_by_position(index, fail=False):
    for column in COLUMNS:
        if index == column.index:
            return column
    if fail: _error('Unknown column [%s]' % column_name)
    return False