#!/usr/bin/env python

from collections import OrderedDict
import pickle
from common import *

## ============================================================================
def _parse_line(c, line, datastore, options):
    """
    """
    fields = line.strip().split('|')
    if len(fields) != len(COLUMNS):
        raise ValueError('%s' % line)
    _debug(fields, options.verbose)
    indexes = datastore['indexes']
    out_fields = []
    for f, field in enumerate(fields):
        column = column_by_position(f)
        out_fields.append((field, column))
        if column.is_index:
            _debug('indexing %s: %s' % (column.name, field), options.verbose)
            key_index = indexes[column.name].get(field, [])
            key_index.append(c)
            indexes[column.name][field] = key_index
    return out_fields

## ============================================================================
def _save(datastore, options):
    """
    """
    _debug('saving datastore %s' % str(datastore), options.verbose)
    ds_file = open('datastore.ds', 'wb')
    pickle.dump(datastore, ds_file)
    ds_file.close()

## ============================================================================
def sort_indexes(datastore, options):
    """
    """
    indexes = datastore['indexes']
    for column_name, index in indexes.iteritems():
        _debug('sorting index %s: %s' % (column_name, str(index)), options.verbose)
        indexes[column_name] = OrderedDict(sorted(index.items(), key=lambda v: v[0]))

## ============================================================================
def import_stream(stream, options):
    """
    """
    data_file = open('data', 'w')
    datastore = {'datafile': 'data', 
                 'indexes': {c.name: {} for c in COLUMNS if c.is_index}}
    for c, line in enumerate(stream):
        _debug('%i: %s' % (c, line.strip()), options.verbose)
        fields = _parse_line(c, line, datastore, options)
        data_file.write(format_output_fields(fields, options))
    # -------------------------------------------------------------------------  
    datastore['num_rows'] = c+1
    sort_indexes(datastore, options)
    _save(datastore, options)
    data_file.close()

## ============================================================================
def format_output_field(field, options):
    """
    """
    return ('{:<%i}' % field[1].size).format(field[0])

## ============================================================================
def format_output_fields(fields, options):
    """
    """
    return ''.join([format_output_field(f, options) for f in fields])

## ============================================================================
if __name__ == '__main__':
    """ Main entry point
    """
    import argparse
    parser = argparse.ArgumentParser(description='Import file or stream to local datastore')
    parser.add_argument('file', type=str, default='-', help='File to import')
    parser.add_argument('--verbose', action='store_true', help='increase output verbosity')
    parser.add_argument('--no_header', action='store_true', default=False, help='process from line 1')
    args = parser.parse_args()
    _debug('reading %s' % args.file, args.verbose)
    stream = open(args.file, 'r') if args.file != '-' else sys.stdin
    if not args.no_header: stream.readline()
    import_stream(stream, args)
    if args.file != '-': stream.close()
