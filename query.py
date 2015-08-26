#!/usr/bin/env python

def import_stream(stream):
    pass

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Query tool')
    parser.add_argument('file', type=str, help='display a square of a given number')
    """
    parser.add_argument('integers', metavar='N', type=int, nargs='+',
                   help='an integer for the accumulator')
    parser.add_argument('--sum', dest='accumulate', action='store_const',
                   const=sum, default=max,
                   help='sum the integers (default: find the max)')
    """
    args = parser.parse_args()
    
