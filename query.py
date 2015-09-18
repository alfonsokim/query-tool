#!/usr/bin/env python

import sys
import pickle
import itertools
from common import *
# used to flatten the rows array from each index
chain = itertools.chain.from_iterable

## ============================================================================
def read_datastore(options):
    """
    """
    ds_file = open(options.datastore, 'rb') 
    datastore = pickle.load(ds_file)
    _debug('Loaded datastore %s' % str(datastore), options.verbose)
    ds_file.close()
    return datastore

## ============================================================================
def build_order_by(order_by, filtered_rows, datastore, options):
    """
    """
    if len(order_by) == 0:
        return filtered_rows
    _debug('Ordering by columns %s' % str(order_by), options.verbose)
    num_rows = 0
    flat_rows = []
    for column in order_by: # Reshape the index rows list to sort by column
        index = datastore['indexes'][column.name]
        # index_rows is a list of tuples in the form (num_row, row)  
        # further sorting must be based on the num_row value,
        # preserving the row value for the actual result
        index_rows = [(i, v) for i, v in enumerate(list(chain(index.itervalues())))
                      if v in filtered_rows]
        flat_rows.extend(index_rows)
        num_rows = len(index_rows)
    # Actual reshape is done over the flat rows, in a slice-n-dice operation
    # to rearange rows in a matrix in the same shape of the resultset
    columns = [flat_rows[row::num_rows] for row in range(num_rows)]
    _debug('columns: %s' % str(columns), options.verbose)
    # Finally, the sorting. From leat-significant column to the most
    # In each row there is a tuple in the form (num_row, row), so the
    # second (1) value of the tuple is used to do the sort.
    for idx in reversed(range(len(order_by))):
        columns.sort(key=lambda c: c[idx][0])
    _debug('columns (sorted): %s' % str(columns), options.verbose)
    # Once the columns are sorted, the most-signinficant row is in index 0
    # and the actual column to fetch is in position 1 of the tuple.
    return [column[0][1] for column in columns]

## ============================================================================
def build_filter(filters, datastore, options):
    """
    """
    if len(filters) == 0:
        return range(datastore['num_rows']) 
    _debug('Filtering by %s' % str(filters), options.verbose)
    filtered_columns = []
    for column, value in filters:
        if column.name not in datastore['indexes']:
            _error('Filtering is only supported on indexed columns (%s)' % column.name)
        index = datastore['indexes'][column.name]
        col_values = index.get(value, []) ## Extender la lista actual
        _debug('Selected columns for value %s: %s' % (value, col_values), options.verbose)
        filtered_columns.extend(col_values)
    return filtered_columns

## ============================================================================
def parse_filter(condition, options):
    """
    """
    items = condition.split('=')
    if len(items) != 2:
        _error('Invalid filter sintax: %s' % condition)
    column = column_by_name(items[0], fail=True)
    return column, items[1]

## ============================================================================
def parse_select_term(term, options):
    """
    """
    if ':' not in term:
        column = column_by_name(term, fail=True)
        return column, ''
    if term.count(':') > 1:
        _error('Invalid aggregate syntax: %s' % term)
    col_name, aggregate = tuple(term.split(':'))
    column = column_by_name(col_name, fail=True)
    _debug('using aggregate %s for column %s' % (aggregate, col_name), options.verbose)
    return column, aggregate

## ============================================================================
def build_plan(datastore, options):
    """
    """
    _debug('creating plan for %s' % options.select, options.verbose)
    columns, indexes, filters, order_by = [], [], [], []
    for column_name in options.select.split(','):
        if '*' == column_name:
            columns.extend([SelectColumn(c) for c in COLUMNS])
        else:
            column, aggregate = parse_select_term(column_name, options)
            columns.append(SelectColumn(column, aggregate))
            if column.is_index: # TODO: Check if this if is needed
                indexes.append(column)
    for column_name in options.order.split(',') if options.order != '' else []:
        column = column_by_name(column_name, fail=True)
        if not column.is_index:
            _error('Ordering by not index column (%s) is not supported' % column.name)
        order_by.append(column)
    for condition in options.filter.split(',') if options.filter != '' else []:
        filters.append(parse_filter(condition, options))
    filtered_rows = build_filter(filters, datastore, options)
    ordered_rows = build_order_by(order_by, filtered_rows, datastore, options)
    return {'columns': columns, 'indexes': indexes, 
            'rows': ordered_rows, 'order_by': order_by}

## ============================================================================
def execute(plan, datastore, options):
    """
    """
    if options.show_plan:
        _debug('Executing plan %s' % str(plan), True)
    datafile = open(datastore['datafile'], 'r')
    for row in plan['rows']:
        _debug('parsing row %i' % row, options.verbose)
        line_begin = row * ROW_SIZE
        for column in plan['columns']:
            datafile.seek(line_begin + column.offset)
            data = datafile.read(column.size).strip()
            column.add_value(data)
            _debug('read [%s] for column %s' % (data, column.name), options.verbose)
    datafile.close()

## ============================================================================
def output_resultset(datastore, plan, options):
    """
    """
    values = zip(*[column.values() for column in plan['columns']])
    output = '\n'.join([','.join(row) for row in values])
    print >> sys.stdout, ','.join([column.format_name() for column in plan['columns']])
    print >> sys.stdout, output
    lr = len(values) # for pretty printing
    print >> sys.stdout, '(%i record%s found)' % (lr, 's' if lr > 1 else '')

## ============================================================================
if __name__ == '__main__':
    """ Main entry point
    """
    import argparse
    parser = argparse.ArgumentParser(description='Query tool')
    parser.add_argument('-d', '--datastore', type=str, default='datastore.ds', 
        metavar='DATASTORE', help='Datastore to use')
    parser.add_argument('-s', '--select', type=str, required=True,
        metavar='COLUMNS', help='columns to select')
    parser.add_argument('-o', '--order', type=str, default='',
        metavar='COLUMNS', help='Order by columns')
    parser.add_argument('-f', '--filter', type=str, default='',
        metavar='CONDITIONS', help='Filter conditions')
    parser.add_argument('--verbose', action='store_true', help='increase verbosity')
    parser.add_argument('--show_plan', action='store_true', help='show query plan')
    args = parser.parse_args()
    datastore = read_datastore(args)
    plan = build_plan(datastore, args)
    execute(plan, datastore, args)
    output_resultset(datastore, plan, args)
