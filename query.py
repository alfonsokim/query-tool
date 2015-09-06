#!/usr/bin/env python

import sys
import pickle
import itertools
from common import *
chain = itertools.chain.from_iterable

## ============================================================================
def read_datastore(options):
    """
    """
    _debug('Reading datastore %s' % options.datastore, options)
    ds_file = open(options.datastore, 'rb') 
    datastore = pickle.load(ds_file)
    _debug('Loaded datastore %s' % str(datastore), options)
    return datastore

## ============================================================================
def build_order_by(order_by, datastore, options):
    """
    """
    if len(order_by) == 0:
        return range(datastore['num_rows']) 
    _debug('Ordering by columns %s' % str(order_by), options)
    num_rows = 0
    flat_rows = []
    for column in order_by:
        index = datastore['indexes'][column.name]
        index_rows = list(chain([v for v in index.itervalues()]))
        num_rows = len(index_rows)
        flat_rows.extend(index_rows)
    _debug('flat_rows (antes): %s' % str(flat_rows), options)
    columns = [flat_rows[row::num_rows] for row in range(num_rows)]
    _debug('cols (despues): %s' % str(columns), options)
    for idx in reversed(range(len(order_by))):
        columns = sorted(columns, key=lambda c: c[idx])
    _debug('cols (sorted): %s' % str(columns), options)
    #return [column[0] for column in columns]
    return range(datastore['num_rows']) 

## ============================================================================
def build_plan(datastore, options):
    """
    """
    _debug('creating plan for %s' % options.select, options)
    columns, indexes, filters, order_by = [], [], [], []
    for column_name in options.select.split(','):
        if '*' == column_name:
            columns.extend(COLUMNS)
        else:
            column = column_by_name(column_name, fail=True)
            columns.append(column)
            if column.is_index:
                indexes.append(column)
    for column_name in options.order.split(',') if options.order != '' else []:
        column = column_by_name(column_name, fail=True)
        if not column.is_index:
            _error('Ordering by not index column (%s) is not supported' % column.name, options)
        order_by.append(column)
    rows = build_order_by(order_by, datastore, options)
    return {'columns': columns, 'indexes': indexes, 
            'rows': rows, 'order_by': order_by}

## ============================================================================
def execute(plan, datastore, options):
    """
    """
    if options.show_plan:
        _debug('Executing plan %s' % str(plan), options, False)
    datafile = open(datastore['datafile'], 'r')
    resultset = []
    for row in plan['rows']:
        _debug('parsing row %i' % row, options)
        result_row = []
        line_begin = row * ROW_SIZE
        for column in plan['columns']:
            datafile.seek(line_begin + column.offset)
            data = datafile.read(column.size).strip()
            result_row.append(data)
            _debug('read [%s] for column %s' % (data, column.name), options)
        resultset.append(result_row)
    datafile.close()
    return resultset

## ============================================================================
def output_resultset(resultset, plan, options):
    """
    """
    _debug('Printing resultset: %s' % str(resultset), options)
    output = '\n'.join([','.join([value for value in row]) for row in resultset])
    print >> sys.stdout, output

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
    parser.add_argument('--verbose', action='store_true', help='increase output verbosity')
    parser.add_argument('--show_plan', action='store_true', help='show query plan')
    args = parser.parse_args()
    datastore = read_datastore(args)
    plan = build_plan(datastore, args)
    resultset = execute(plan, datastore, args)
    output_resultset(resultset, plan, args)
