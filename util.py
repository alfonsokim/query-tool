
__all__ = ['STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME', 
           'COLUMNS', 'column_by_name', 'error', 'ROW_SIZE',
           'debug', 'column_by_position', 'SelectColumn']

from datetime import datetime as dt
from collections import namedtuple
import sys

## ============================================================================
# Classes for parsing and formatting the column values from the datastore
# __init__: Parses raw data from data file to the column value
# format: Converts column value to the output format
## ============================================================================
class Char():

    def __init__(self, value):
        self.value = value

    def format(self):
        return self.value

## ----------------------------------------------------------------------------
class Date():

    def __init__(self, value):
        self.value = dt.strptime(value, '%Y-%m-%d')

    def format(self):
        return self.value.strftime('%Y-%m-%d')

## ----------------------------------------------------------------------------
class Money():

    def __init__(self, value):
        dollars, cents = tuple(value.split('.'))
        self.value = (int(dollars) * 100) + int(cents)

    def format(self):
        return '%.2f' % (self.value / 100)

## ----------------------------------------------------------------------------
class Time():

    def __init__(self, value):
        hours, minutes = tuple(value.split(':'))
        self.value = (int(hours) * 60) + int(minutes)

    def format(self):
        return '%i:%02i' % (self.value / 60, self.value % 60)


## ============================================================================
Column = namedtuple('Column', 'name index is_index size offset type')
# Current schema of the data file. Further work can be parsing the table
# definition from a SQL CREATE TABLE syntax or something similar
## ----------------------------------------------------------------------------
STB = Column(name='STB', index=0, is_index=True, size=64, offset=0, type=Char)
TITLE = Column(name='TITLE', index=1, is_index=True, size=64, offset=64, type=Char)
PROVIDER = Column(name='PROVIDER', index=2, is_index=True, size=64, offset=128, type=Char)
DATE = Column(name='DATE', index=3, is_index=True, size=10, offset=192, type=Date)
REV = Column(name='REV', index=4, is_index=True, size=10, offset=202, type=Money)
VIEW_TIME = Column(name='VIEW_TIME', index=5, is_index=True, size=10, offset=212, type=Time)
COLUMNS = [STB, TITLE, PROVIDER, DATE, REV, VIEW_TIME]
ROW_SIZE = sum([c.size for c in COLUMNS])

AGGREGATES = 'min,max,sum,count,collect,'.split(',')
COLUMN_PROPERTIES = 'name index is_index size offset'.split()

## ============================================================================
class SelectColumn(): ## TODO: Cambiar de lugar esta clase
    """ SelectColumn holds a column, the aggregate and the data, these objects
        are used in the query process to process the column data and
        aggregates 
    """

    # -------------------------------------------------------------------------
    def __init__(self, column, aggregate=''):
        """ :param column: The column (from the named tuple) 
            :param aggregate: The aggregate, if it was specified
        """
        self.column = column
        self.aggregate = aggregate
        self.raw_values = None
        self.current_value = None
        if aggregate not in AGGREGATES:
            error('invalid aggregate: %s' % aggregate)
        if aggregate == 'sum' and column.type in [Date, Char]:
            error('Cannot add on column %s' % column.name)
        if aggregate == '':
            self.current_value = []

    # -------------------------------------------------------------------------
    def __getattr__(self, attr):
        """ Proxy for the column's properties

            :returns: A property from the column namedtuple
        """
        return self.column[COLUMN_PROPERTIES.index(attr)]

    # -------------------------------------------------------------------------
    def add_value(self, value):
        """ This method is called when a new value is read from the local 
            datastore. Then the value is processed according to the aggregate
            property in the select statement

            :param value: Raw value from the datastore
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
            else: # check if new_value is in values set
                if new_value.value not in self.raw_values:
                    self.raw_values.add(new_value.value)
                    self.current_value.append(new_value)

    # -------------------------------------------------------------------------
    def values(self):
        """ Get the values of the current column

            :returns: A list with the computed values of the column
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
        """ Utility method for printing the column name and the aggregate
            in the result 

            :returns: The column definition
        """
        if self.aggregate != '':
            return ':'.join([self.column.name, self.aggregate])
        else:
            return self.column.name

    # -------------------------------------------------------------------------
    def __repr__(self):
        return str(self)

    # -------------------------------------------------------------------------
    def __str__(self):
        return 'Column: %s, Aggregate: %s' % (self.column, self.aggregate)

## ============================================================================
def column_by_name(column_name, fail=False):
    """ Finds a column for the given column name

        :param column_name: The name of the column to find
        :param fail: calls error if no column is found
        :returns: The column with the given name, empty if not found
    """
    for column in COLUMNS:
        if column_name.upper() == column.name:
            return column
    if fail: error('Unknown column [%s]' % column_name)
    return False

## ============================================================================
def column_by_position(index, fail=False):
    """ Finds a column for the given position

        :param index: The index of the column to find
        :param fail: calls error if no column is found
        :returns: The column with the given index, empty if not found
    """
    for column in COLUMNS:
        if index == column.index:
            return column
    if fail: error('Unknown column index [%i]' % index)
    return False

## ============================================================================
def debug(message, is_verbose):
    """ Prints a debug message in the error output
    """
    if is_verbose:
        print >> sys.stderr, 'DEBUG: %s' % message

## ============================================================================
def error(message):
    """Prints an error message in the error output and exits
    """
    print >> sys.stderr, message
    sys.exit(1)
