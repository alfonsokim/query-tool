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
def build_plan(options):
    """
    """
    _debug('creating plan for %s' % options.select, options)
    columns = []
    for column_name in options.select.split(','):
        column = column_by_name(column_name)
        if not column:
            _error('Unknown column [%s]' % column_name, options)
        columns.append(column)
    _debug('columns to select: %s' % str(columns), options)
    

## ============================================================================
if __name__ == '__main__':
    """
    """
    import argparse
    parser = argparse.ArgumentParser(description='Query tool')
    parser.add_argument('-d', '--datastore', type=str, default='datastore.ds', 
        metavar='DATASTORE', help='Datastore to use')
    parser.add_argument('-s', '--select', type=str, required=True,
        metavar='COLUMNS', help='columns to select')
    parser.add_argument('--verbose', action='store_true', help='increase output verbosity')
    args = parser.parse_args()
    datastore = read_datastore(args)
    plan = build_plan(args)
