#!/usr/bin/env python

from collections import namedtuple

## ============================================================================
Column = namedtuple('Column', 'name index size')
STB = Column('STB', 0, 64)
TITLE = Column('TITLE', 1, 64)
PROVIDER = Column('PROVIDER', 2, 64)
DATE = Column('DATE', 3, 64)
REV = Column('REV', 4, 64)
VIEW_TIME = Column('VIEW_TIME', 5, 64)
LINE_ITEMS = 6
INDEX_COLUMNS = [STB.index, TITLE.index]
COLUMN_NAMES = 'STB|TITLE|PROVIDER|DATE|REV|VIEW_TIME'.split('|')

## ============================================================================
def _debug(line, options):
    """
    """
    if options.verbose:
        print >> sys.stderr, line

## ============================================================================

def _parse_line(c, line, datastore, options):
    """
    """
    fields = line.strip().split('|')
    if len(fields) != LINE_ITEMS:
        raise ValueError('%s' % line)
    _debug(fields, options)
    indexes = datastore['indexes']
    for f, field in enumerate(fields):
        if f in INDEX_COLUMNS:
            _debug('indexando %s: %s' % (COLUMN_NAMES[f], field), options)
            key_index = indexes[COLUMN_NAMES[f]].get(field, [])
            key_index.append(c)
            indexes[COLUMN_NAMES[f]][field] = key_index

## ============================================================================
def import_stream(stream, options):
    """
    """
    data_file = open('data', 'wb')
    index_file = open('index', 'w')
    datastore = {'d': data_file, 'i': index_file, 
                 'indexes': {COLUMN_NAMES[f]: {} for f in INDEX_COLUMNS}}
    print datastore
    for c, line in enumerate(stream):
        _debug('%i: %s' % (c, line.strip()), options)
        _parse_line(c, line, datastore, options)
    # -------------------------------------------------------------------------
    _debug('datastore: %s' % str(datastore), options)    
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
