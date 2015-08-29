#!/usr/bin/env python

import sys
import pickle
from common import *

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
def build_plan(datastore, options):
    """
    """
    _debug('creating plan for %s' % options.select, options)
    columns = []
    indexes = []
    filters = []
    rows = []
    for column_name in options.select.split(','):
        if '*' == column_name:
            columns.extend(COLUMNS)
        else:
            column = column_by_name(column_name)
            if not column:
                _error('Unknown column [%s]' % column_name, options)
            columns.append(column)
            if column.is_index:
                indexes.append(column)
    for column_name in options.order.split(',') if options.order != '' else []:
        _debug('ordering by column %s' % column_name, options)
        column = column_by_name(column_name)
        if not column:
                _error('Unknown column [%s]' % column_name, options)
        if column.is_index:
            column_order = datastore['indexes'][column.name]
        else:
            pass ## TODO: What to do if a column is not index
    rows = range(datastore['num_rows']) ## TODO: Si hay condicionales filtrar las columnas
    return {'columns': columns, 'indexes': indexes, 'rows': rows}

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
def output_resultset(resultset, options):
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
    output_resultset(resultset, args)
