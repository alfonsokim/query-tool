#!/usr/bin/env python

## ============================================================================
STB_IDX = 0
TITLE_IDX = 1
PROVIDER_IDX = 2
DATE_IDX = 3
REV_IDX = 4
VIEW_TIME_IDX = 5
LINE_ITEMS = 6

## ============================================================================
def _debug(line, options):
    """
    """
    if options.verbose:
        print >> sys.stderr, line

## ============================================================================

def _import_line(line, datastore, options):
    """
    """
    fields = line.strip().split('|')
    if len(fields) != LINE_ITEMS:
        raise ValueError('%s' % line)
    _debug(fields, options)

## ============================================================================
def import_stream(stream, options):
    """
    """
    data_file = open('data', 'w')
    index_file = open('index', 'w')
    datastore = {'d': data_file, 'i': index_file}
    for c, line in enumerate(stream):
        _debug('%i: %s' % (c, line.strip()), options)
        _import_line(line, datastore, options)
    # -------------------------------------------------------------------------    
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
    args = parser.parse_args()
    _debug('reading %s' % args.file, args)
    stream = open(args.file, 'r') if args.file != '-' else sys.stdin
    import_stream(stream, args)
    if args.file != '-': stream.close()
