#!/usr/bin/env python

import pickle
from common import *

## ============================================================================

def _parse_line(c, line, datastore, options):
    """
    """
    fields = line.strip().split('|')
    if len(fields) != len(COLUMNS):
        raise ValueError('%s' % line)
    _debug(fields, options)
    indexes = datastore['indexes']
    for f, field in enumerate(fields):
        if f in INDEX_COLUMNS:
            column = COLUMNS[f]
            _debug('indexando %s: %s' % (column.name, field), options)
            key_index = indexes[column.name].get(field, [])
            key_index.append(c)
            indexes[column.name][field] = key_index

## ============================================================================
def _save(datastore, options):
    """
    """
    _debug('saving datastore %s' % str(datastore), options)
    ds_file = open('datastore.ds', 'wb')
    pickle.dump(datastore, ds_file)
    ds_file.close()

## ============================================================================
def format_output_line(line, options):
    """
    """
    return line.strip()

## ============================================================================
def import_stream(stream, options):
    """
    """
    data_file = open('data', 'w')
    index_file = open('index', 'w')
    datastore = {'datafile': 'data', 
                 'indexes': {COLUMNS[f].name: {} for f in INDEX_COLUMNS}}
    for c, line in enumerate(stream):
        _debug('%i: %s' % (c, line.strip()), options)
        _parse_line(c, line, datastore, options)
        print >> data_file, format_output_line(line, options)
    # -------------------------------------------------------------------------  
    _save(datastore, options)
    data_file.close()
    index_file.close()

## ============================================================================
if __name__ == '__main__':
    """ Main entry point
    """
    import argparse
    import sys
    parser = argparse.ArgumentParser(description='Import file or stream to local datastore')
    parser.add_argument('file', type=str, default='-', help='File to import')
    #parser.add_argument('separator', type=str, default='|', help='Field separator')
    parser.add_argument('--verbose', action='store_true', help='increase output verbosity')
    parser.add_argument('--no_header', action='store_true', default=False, help='process from line 1')
    args = parser.parse_args()
    _debug('reading %s' % args.file, args)
    stream = open(args.file, 'r') if args.file != '-' else sys.stdin
    if not args.no_header: stream.readline()
    import_stream(stream, args)
    if args.file != '-': stream.close()
