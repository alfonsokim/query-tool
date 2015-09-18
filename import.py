#!/usr/bin/env python

from collections import OrderedDict
import pickle
import sys
from util import *

## ============================================================================
def parse_line(c, line, datastore, options):
    """ Parse a line of the stream

        :param c: Line index
        :param line: Current line 
        :param datastore: Local datastore to save the line
        :returns: Parsed fields from the line
    """
    fields = line.strip().split('|')
    if len(fields) != len(COLUMNS):
        error('Invalid line: %s' % line.strip())
    debug('parsing line %i: %s' % (c, str(fields)), options.verbose)
    indexes = datastore['indexes']
    out_fields = []
    for f, field in enumerate(fields):
        column = column_by_position(f)
        out_fields.append((field, column))
        if column.is_index:
            debug('indexing %s: %s' % (column.name, field), options.verbose)
            key_index = indexes[column.name].get(field, [])
            key_index.append(c)
            indexes[column.name][field] = key_index
    return out_fields

## ============================================================================
def save_datastore(datastore, options):
    """ Saves current datastore to the local filesystem, for further reading
        in the query process.

        :param datastore: Datastore to save
        :param options: Options dictionary from command line args
    """
    msg = 'saving datastore %s to file %s.ds' 
    debug(msg % (str(datastore), options.output), options.verbose)
    ds_file = open('%s.ds' % options.output, 'wb')
    pickle.dump(datastore, ds_file)
    ds_file.close()

## ============================================================================
def sort_indexes(datastore, options):
    """ Sort the processed indexes by fields value
        :param datastore: Current datastore 
        :param options: Options dictionary from command line args
    """
    indexes = datastore['indexes']
    for column_name, index in indexes.iteritems():
        debug('sorting index %s: %s' % (column_name, str(index)), options.verbose)
        indexes[column_name] = OrderedDict(sorted(index.items(), key=lambda v: v[0]))

## ============================================================================
def import_stream(stream, options):
    """ Imports a pipe separated stream to the local data store

        :param stream: The stream to import, data separated by pipes
        :param options: Options dictionary from command line args
    """
    data_file = open(options.output, 'w')
    datastore = {'datafile': options.output, 
                 'indexes': {c.name: {} for c in COLUMNS if c.is_index}}
    for c, line in enumerate(stream):
        fields = parse_line(c, line, datastore, options)
        data_file.write(format_output_fields(fields, options))
    # -------------------------------------------------------------------------  
    datastore['num_rows'] = c+1
    sort_indexes(datastore, options)
    save_datastore(datastore, options)
    data_file.close()

## ============================================================================
def format_output_field(field, options):
    """ Format a field to save in the datastore

        :param fieldf: A field to format
        :param options: Options dictionary from command line args
        :returns: A string in the the datastore format, based on the
                  field length
    """
    return ('{:<%i}' % field[1].size).format(field[0])

## ============================================================================
def format_output_fields(fields, options):
    """ Format a list of fields to save in the datastore

        :param fields: List of fields to format
        :param options: Options dictionary from command line args
        :returns: A string in the the datastore format
    """
    return ''.join([format_output_field(f, options) for f in fields])

## ============================================================================
if __name__ == '__main__':
    """ Program entry point
        Builds the argument parser and run the import sequence
    """
    import argparse
    parser = argparse.ArgumentParser(description='Import file or stream to local datastore')
    parser.add_argument('file', type=str, default='-', help='File to import')
    parser.add_argument('-o', '--output', type=str, default='data', 
        metavar='OUTPUT', help='Name of the new datastore')
    parser.add_argument('--verbose', action='store_true', help='show debug messages')
    parser.add_argument('--no_header', action='store_true', default=False, help='process from line 1')
    args = parser.parse_args()
    debug('reading %s' % args.file, args.verbose)
    stream = open(args.file, 'r') if args.file != '-' else sys.stdin
    if not args.no_header: stream.readline()
    import_stream(stream, args)
    if args.file != '-': stream.close()
